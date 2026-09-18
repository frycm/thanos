// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/compact"
)

// Run is one bucket and the compactor process under test over it, with the
// participants' fault-injectable views. A feature that adds participants, such
// as workers, registers their views with AddView and describes when it is
// idle with Quiet.
type Run struct {
	T      *testing.T
	Shared objstore.Bucket
	Corpus *Corpus
	Conf   NodeConfig

	// NewNode builds the process under test for a configuration. The default
	// is a standalone node.
	NewNode func(conf NodeConfig) *Node
	// Quiet reports whether nothing is in flight outside the current node,
	// such as tasks leased to workers. nil means always quiet.
	Quiet func() bool

	mtx   sync.Mutex
	node  *Node
	views []*HookBucket
	// replacing counts Replace calls in progress: the old node is dead and
	// its successor not yet in place, so a convergence loop has to wait.
	replacing int
	// background tracks the scenario's fault-injection goroutines, so that a
	// scenario never ends with one still running.
	background sync.WaitGroup
}

// NewRun uploads the corpus into a fresh in-memory bucket and starts a
// standalone node over it. Features that replace NewNode do so before calling
// Start.
func NewRun(t *testing.T, c *Corpus, conf NodeConfig) *Run {
	t.Helper()
	r := &Run{T: t, Shared: objstore.NewInMemBucket(), Corpus: c, Conf: conf.WithDefaults()}
	r.NewNode = func(conf NodeConfig) *Node { return NewNode(t, r.Shared, conf, Hooks{}) }
	c.Upload(t, r.Shared)
	return r
}

// Start builds the first node. NewRun does not, so that a feature can install
// its own NewNode first.
func (r *Run) Start() *Run {
	r.T.Helper()
	n := r.NewNode(r.Conf)
	r.mtx.Lock()
	r.node = n
	r.mtx.Unlock()
	r.AddView(n.Bkt)
	return r
}

// Current returns the node under test.
func (r *Run) Current() *Node {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.node
}

// Replace stops the running node mid-pass and starts another with the given
// configuration, as a restart would. The process dies first, and only then
// does its replacement start: a crashed process cannot reach the bucket, so
// whatever goroutines of the old node are still winding down - a journal
// write racing the successor's takeover, say - must not. A convergence loop
// waits for the successor meanwhile.
func (r *Run) Replace(conf NodeConfig) *Node {
	r.T.Helper()
	old := r.Current()
	r.mtx.Lock()
	r.replacing++
	r.mtx.Unlock()
	old.AbortIteration()
	old.Stop()
	old.Bkt.Fence()
	n := r.NewNode(conf.WithDefaults())
	r.mtx.Lock()
	r.node = n
	r.replacing--
	r.mtx.Unlock()
	r.AddView(n.Bkt)
	return n
}

// Replacing reports whether a Replace is in progress: the current node is
// dead and its successor not yet in place.
func (r *Run) Replacing() bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.replacing > 0
}

// Install makes a node built outside Replace the current one without stopping
// its predecessor, as a second process started by mistake over the same
// bucket would be.
func (r *Run) Install(n *Node) {
	r.mtx.Lock()
	r.node = n
	r.mtx.Unlock()
	r.AddView(n.Bkt)
}

// AddView registers a participant's view of the bucket, so that faults meant
// for every participant reach it.
func (r *Run) AddView(v *HookBucket) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.views = append(r.views, v)
}

// Views returns every registered view.
func (r *Run) Views() []*HookBucket {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]*HookBucket(nil), r.views...)
}

// Inject runs a fault-injection routine in the background; the test waits for
// it to end before it finishes.
func (r *Run) Inject(f func()) {
	r.background.Go(f)
}

// Wait blocks until every fault-injection routine has ended.
func (r *Run) Wait() {
	r.background.Wait()
}

// WaitFor polls until cond holds.
func (r *Run) WaitFor(msg string, cond func() bool) {
	r.T.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.T.Fatalf("timed out waiting for %s", msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Fingerprint summarizes the shared bucket.
func (r *Run) Fingerprint() string { return Fingerprint(r.Shared) }

// Passes runs up to n single passes of the current node, for a control loop
// that is not expected to settle, such as one whose group fails on every
// pass. It stops early when a pass halts, and returns the last error.
func (r *Run) Passes(n int) error {
	r.T.Helper()
	var err error
	for i := 1; i <= n; i++ {
		node := r.Current()
		err = node.Iterate(context.Background())
		if err != nil {
			r.T.Logf("pass %d: %v", i, err)
			if compact.IsHaltError(err) || node.Halted.Load() {
				return err
			}
		}
	}
	return err
}

// PassUntil runs single passes of the current node until cond holds, failing
// the test if a pass halts.
func (r *Run) PassUntil(msg string, cond func() bool) {
	r.T.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for i := 1; !cond(); i++ {
		if time.Now().After(deadline) {
			r.T.Fatalf("timed out waiting for %s", msg)
		}
		n := r.Current()
		if err := n.Iterate(context.Background()); err != nil {
			testutil.Assert(r.T, !compact.IsHaltError(err) && !n.Halted.Load(), "the node halted: %v", err)
			r.T.Logf("pass %d: %v", i, err)
		}
	}
}

// ConvergeResult describes how a convergence loop ended.
type ConvergeResult struct {
	Halted     bool
	Iterations int
	LastErr    error
}

// Converge runs passes of the current node until two consecutive passes
// changed nothing and the run is quiet, the way the binary's wait loop would
// keep going. Retryable and canceled passes are logged and retried; a halt
// ends the run, as it would end the binary's.
func (r *Run) Converge(timeout time.Duration) ConvergeResult {
	r.T.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	stable := 0
	res := ConvergeResult{}
	for {
		n := r.Current()
		err := n.Iterate(context.Background())
		res.Iterations++
		res.LastErr = err
		if n.Stopped() {
			// Stopped from outside, as a scenario killing the process does. A
			// dead process reports nothing, so whatever the pass returned is
			// not a verdict; with no successor the run ends here, otherwise
			// the successor carries on, once it is in place.
			if r.Current() == n && !r.Replacing() {
				return res
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err != nil && compact.IsHaltError(err) || n.Halted.Load() {
			res.Halted = true
			return res
		}
		if err != nil {
			r.T.Logf("pass %d: %v", res.Iterations, err)
		}
		quiet := r.Quiet == nil || r.Quiet()
		fp := r.Fingerprint()
		if err == nil && quiet && fp == last {
			stable++
		} else {
			stable = 0
		}
		last = fp
		if stable >= 2 {
			return res
		}
		if time.Now().After(deadline) {
			r.T.Fatalf("the bucket did not converge within %s after %d passes; last error: %v", timeout, res.Iterations, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Converged runs to convergence and asserts the outcome every scenario
// shares: no halt, the bucket holds what the oracle says, nothing overlaps.
func Converged(t *testing.T, r *Run, want *BucketDump) *BucketDump {
	t.Helper()
	res := r.Converge(90 * time.Second)
	testutil.Assert(t, !res.Halted, "the compactor halted: %v", res.LastErr)
	got := DumpBucket(t, r.Shared)
	AssertSameContent(t, want, got, "after convergence")
	got.AssertNoOverlaps(t)
	return got
}

// Golden runs the standalone compactor over the corpus to convergence and
// returns what it left behind. It is the oracle every run is judged against,
// whatever executes that run's plans.
func Golden(t *testing.T, c *Corpus, conf NodeConfig) *BucketDump {
	t.Helper()
	r := NewRun(t, c, conf).Start()
	res := r.Converge(2 * time.Minute)
	testutil.Assert(t, !res.Halted, "the standalone compactor halted on the corpus: %v", res.LastErr)
	d := DumpBucket(t, r.Shared)
	t.Logf("standalone layout after %d passes:\n  %s", res.Iterations, strings.Join(d.Layout(), "\n  "))
	return d
}

// InputDump is the corpus as uploaded, before any compactor touched it.
func InputDump(t *testing.T, c *Corpus) *BucketDump {
	t.Helper()
	bkt := objstore.NewInMemBucket()
	c.Upload(t, bkt)
	return DumpBucket(t, bkt)
}

// Scenario is one fault to survive. Every scenario starts from the same
// uploaded corpus and ends by judging the bucket, most of them against the
// standalone compactor's result for the same blocks.
type Scenario struct {
	Name string
	// Tenants overrides the corpus; nil means HACorpus.
	Tenants []TenantSpec
	// Conf adjusts the configuration; nil keeps the suite's.
	Conf func(NodeConfig) NodeConfig
	Run  func(t *testing.T, r *Run, want *BucketDump, input *BucketDump)
}

// Suite describes what to run the scenarios against.
type Suite struct {
	// NewRun builds the run under test with the corpus uploaded and the
	// first node started. nil means a standalone run.
	NewRun func(t *testing.T, c *Corpus, conf NodeConfig) *Run
	// Conf is the base configuration; the zero value means HANodeConfig.
	Conf NodeConfig
	// Scenarios to run in addition to the generic ones. Extra alone runs
	// only these.
	Scenarios []Scenario
	Extra     bool
	// OracleConf derives the oracle's configuration from a scenario's. nil
	// judges a run against the standalone compactor under the same
	// configuration; a feature that changes block layout but not content,
	// such as splitting, judges itself against the compactor without it.
	OracleConf func(NodeConfig) NodeConfig
}

// RunSuite runs the generic scenarios and the suite's own against the suite's
// run. The oracle is always the standalone compactor over the same corpus and
// configuration, computed once per corpus and configuration.
func RunSuite(t *testing.T, suite Suite) {
	t.Helper()
	SkipUnlessScenarios(t)

	newRun := suite.NewRun
	if newRun == nil {
		newRun = func(t *testing.T, c *Corpus, conf NodeConfig) *Run { return NewRun(t, c, conf).Start() }
	}
	base := suite.Conf
	if len(base.DedupReplicaLabels) == 0 && base.DedupFunc == "" {
		base = HANodeConfig()
	}
	scenarios := suite.Scenarios
	if !suite.Extra {
		scenarios = append(Scenarios(), scenarios...)
	}

	defaultCorpus := BuildCorpus(t, "ha", HACorpus())
	goldens := map[string]*BucketDump{}
	inputs := map[string]*BucketDump{}

	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			conf := base
			if sc.Conf != nil {
				conf = sc.Conf(conf)
			}
			conf = conf.WithDefaults()

			c := defaultCorpus
			if sc.Tenants != nil {
				c = BuildCorpus(t, sc.Name, sc.Tenants)
			}
			oracle := conf
			if suite.OracleConf != nil {
				oracle = suite.OracleConf(conf).WithDefaults()
			}
			key := fmt.Sprintf("%s/%+v", c.Name, oracle)
			if goldens[key] == nil {
				goldens[key] = Golden(t, c, oracle)
				inputs[key] = InputDump(t, c)
				if c == defaultCorpus {
					// The corpus spans 48h, so the oracle must cover the
					// downsampled level too, or half the machinery is untested.
					var downsampled int
					for _, b := range goldens[key].Blocks {
						if b.Res > 0 {
							downsampled++
						}
					}
					testutil.Assert(t, downsampled > 0, "the standalone run left no downsampled block; the corpus is too short")
				}
			}

			r := newRun(t, c, conf)
			t.Cleanup(r.Wait)
			sc.Run(t, r, goldens[key], inputs[key])
			r.Wait()
		})
	}
}

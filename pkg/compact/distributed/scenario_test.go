// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"cmp"
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// This file is the fault scenario suite for the distributed compactor: the
// whole control loop of the binary - sync, grouping, planning, dispatch,
// verification, garbage collection and both downsampling passes - run in
// process against a synthetic bucket, with faults injected between the pieces,
// and the outcome judged against what the standalone compactor produces from
// the same blocks.
//
// The corpus, the content oracle, the fault-injectable bucket views and the
// compactor process itself come from pkg/compact/compacttest, which also holds
// the scenarios every compactor has to survive; TestGenericScenarios runs
// those against a manager and its workers. This file adds what only the
// distributed compactor can get wrong: workers crashing, managers restarting
// or racing on one journal, an unreachable journal, parked tasks, rollback.
//
// The suite is slow and off by default. Run it with
//
//	go test ./pkg/compact/distributed/ -run TestScenarios -race -args -distributed.scenarios
//
// or with THANOS_DISTRIBUTED_SCENARIOS set in the environment.

var runScenarios = flag.Bool("distributed.scenarios", false, "Run the in-process fault scenario suite for the distributed compactor. Slow; off by default.")

func skipUnlessScenarios(t *testing.T) {
	t.Helper()
	if *runScenarios || os.Getenv("THANOS_DISTRIBUTED_SCENARIOS") != "" {
		t.Setenv("THANOS_COMPACT_SCENARIOS", "1")
	}
	compacttest.SkipUnlessScenarios(t)
}

// modeStandalone and modeManager are the modes one compactor process of a
// scenario runs in.
const (
	modeStandalone = "standalone"
	modeManager    = "manager"
)

type nodeConfig struct {
	compacttest.NodeConfig
	mode          string
	journalID     string
	leaseTTL      time.Duration
	maxAttempts   int
	maxInflight   int
	maxTaskSeries uint64
}

func (c nodeConfig) withDefaults() nodeConfig {
	c.NodeConfig = c.WithDefaults()
	c.mode = cmp.Or(c.mode, modeManager)
	c.journalID = cmp.Or(c.journalID, "scenario-shard")
	c.leaseTTL = cmp.Or(c.leaseTTL, 250*time.Millisecond)
	c.maxAttempts = cmp.Or(c.maxAttempts, 3)
	c.maxInflight = cmp.Or(c.maxInflight, 4)
	return c
}

// haNodeConfig is the manager configuration of the deployment the suite
// exists for: penalty deduplication over the receive replica label.
func haNodeConfig() nodeConfig {
	return nodeConfig{NodeConfig: compacttest.HANodeConfig()}
}

// node is one compactor process, standalone or manager, wired exactly as
// cmd/thanos/compact.go wires it. The manager adds the scheduler, its HTTP
// handlers and the maintenance loop to the shared compactor process.
type node struct {
	*compacttest.Node
	conf  nodeConfig
	sched *Scheduler
	// Closed after the maintenance writer exits, so an orderly configuration
	// change can obey the same single-manager contract as a deployment.
	maintenanceDone    chan struct{}
	downsamples        *prometheus.CounterVec
	downsampleFailures *prometheus.CounterVec
}

func newNode(t *testing.T, shared objstore.Bucket, handler *switchableHandler, conf nodeConfig) *node {
	t.Helper()
	conf = conf.withDefaults()
	n := &node{
		conf:               conf,
		downsamples:        promauto.With(nil).NewCounterVec(prometheus.CounterOpts{Name: "scenario_downsamples"}, []string{"resolution"}),
		downsampleFailures: promauto.With(nil).NewCounterVec(prometheus.CounterOpts{Name: "scenario_downsample_failures"}, []string{"resolution"}),
	}
	var hooks compacttest.Hooks
	if conf.mode == modeManager {
		hooks.Executor = func(cn *compacttest.Node, planner compact.Planner) compact.PlanExecutor {
			sched, err := NewScheduler(cn.Ctx, cn.Logger, cn.Bkt, cn.Reg, ManagerConfig{
				JournalID:          conf.journalID,
				DedupFunc:          conf.DedupFunc,
				DedupReplicaLabels: conf.DedupReplicaLabels,
				LeaseTTL:           conf.leaseTTL,
				MaxAttempts:        conf.maxAttempts,
				MaxTaskSeries:      conf.maxTaskSeries,
			})
			testutil.Ok(t, err)
			n.sched = sched
			cn.DedupFilter.SetPublishedFunc(sched.PublishedFunc())

			mux := http.NewServeMux()
			RegisterServer(mux, cn.Logger, sched)
			handler.swap(mux)

			// The maintenance loop of the binary: expire leases, prune, unpark.
			// A halt found here ends the manager, as it ends the process.
			n.maintenanceDone = make(chan struct{})
			go func() {
				defer close(n.maintenanceDone)
				_ = runutil.Repeat(conf.leaseTTL/4, cn.Ctx.Done(), func() error {
					err := sched.Maintain()
					if err == nil {
						return nil
					}
					if !compact.IsHaltError(err) {
						level.Warn(cn.Logger).Log("msg", "maintenance tick failed; retrying on the next tick", "err", err)
						return nil
					}
					cn.Halted.Store(true)
					cn.AbortIteration()
					return err
				})
			}()
			return NewRemotePlanExecutor(cn.Logger, cn.Bkt, sched, planner, conf.maxInflight, nil)
		}
		hooks.Downsample = n.downsample
	}
	n.Node = compacttest.NewNode(t, shared, conf.NodeConfig, hooks)
	return n
}

// scenarioRun is one bucket, one manager behind a stable URL, and the workers
// a scenario starts against it.
type scenarioRun struct {
	*compacttest.Run
	t       *testing.T
	handler *switchableHandler
	srv     *httptest.Server
	conf    nodeConfig

	mtx     sync.Mutex
	manager *node
	// next is the configuration the next node built by the run gets; the
	// shared run only knows the compactor part of it.
	next    nodeConfig
	workers []*scnWorker
}

func newScenarioRun(t *testing.T, c *compacttest.Corpus, conf nodeConfig) *scenarioRun {
	t.Helper()
	s := &scenarioRun{
		t:       t,
		handler: &switchableHandler{},
		conf:    conf.withDefaults(),
	}
	s.next = s.conf
	s.handler.swap(http.NotFoundHandler())
	s.srv = httptest.NewServer(s.handler)
	t.Cleanup(s.srv.Close)
	s.Run = compacttest.NewRun(t, c, s.conf.NodeConfig)
	s.NewNode = func(base compacttest.NodeConfig) *compacttest.Node {
		s.mtx.Lock()
		nodeConf := s.next
		s.mtx.Unlock()
		nodeConf.NodeConfig = base
		n := newNode(t, s.Shared, s.handler, nodeConf)
		s.mtx.Lock()
		s.manager = n
		s.mtx.Unlock()
		return n.Node
	}
	s.Quiet = func() bool {
		return s.currentManager().sched == nil || len(s.tasksInState(StatePending))+len(s.tasksInState(StateLeased)) == 0
	}
	s.Start()
	return s
}

func (s *scenarioRun) currentManager() *node {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.manager
}

// replaceManager stops the running manager mid-pass and starts another with
// the given configuration on the same journal, as a restart or a takeover
// would. The new manager bumps the generation, so every lease of the old one
// is void.
func (s *scenarioRun) replaceManager(conf nodeConfig) *node {
	s.t.Helper()
	conf = conf.withDefaults()
	s.mtx.Lock()
	s.next = conf
	s.mtx.Unlock()
	s.Replace(conf.NodeConfig)
	return s.currentManager()
}

// install makes a manager built outside the run the current one without
// stopping its predecessor, as a second manager started by mistake would be.
func (s *scenarioRun) install(n *node) {
	s.mtx.Lock()
	s.manager = n
	s.mtx.Unlock()
	s.Install(n.Node)
}

type workerOpts struct {
	journalID          string
	dedupFunc          string
	dedupReplicaLabels []string
}

// scnWorker is a testWorker with a kill switch: crash stops the worker AND
// drops anything it would still report, the way a dead process reports
// nothing. A plain cancel is a shutdown; a crash must look like a vanishing.
type scnWorker struct {
	*testWorker
	dead *atomic.Bool
}

func (w *scnWorker) crash() {
	w.dead.Store(true)
	w.cancel()
	<-w.done
}

// startWorker runs a real worker against the manager URL with the manager's
// deduplication configuration, unless the options say otherwise.
func (s *scenarioRun) startWorker(id string, opts ...workerOpts) *scnWorker {
	s.t.Helper()
	o := workerOpts{journalID: s.conf.journalID, dedupFunc: s.conf.DedupFunc, dedupReplicaLabels: s.conf.DedupReplicaLabels}
	if len(opts) > 0 {
		o = opts[0]
	}
	w := &scnWorker{
		testWorker: &testWorker{
			id:   id,
			bkt:  compacttest.NewHookBucket(s.Shared),
			reg:  prometheus.NewRegistry(),
			done: make(chan struct{}),
		},
		dead: &atomic.Bool{},
	}
	logger := log.NewNopLogger()
	comp, err := tsdb.NewLeveledCompactor(context.Background(), w.reg, logutil.GoKitLogToSlog(logger), compacttest.Levels, downsample.NewPool(), compacttest.MergeFuncFor(o.dedupFunc))
	testutil.Ok(s.t, err)

	client := NewHTTPClient(
		logger,
		dns.NewProvider(logger, prometheus.NewRegistry(), dns.GolangResolverType),
		strings.TrimPrefix(s.srv.URL, "http://"),
		5*time.Second,
	)
	worker, err := NewWorker(logger, w.bkt, crashableClient{TaskClient: client, dead: w.dead}, comp, w.reg, WorkerConfig{
		WorkerID:           id,
		JournalID:          o.journalID,
		DedupFunc:          o.dedupFunc,
		DedupReplicaLabels: o.dedupReplicaLabels,
		DataDir:            s.t.TempDir(),
		PollInterval:       25 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
	})
	testutil.Ok(s.t, err)

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	s.t.Cleanup(cancel)
	go func() {
		defer close(w.done)
		_ = worker.Run(ctx)
	}()
	s.mtx.Lock()
	s.workers = append(s.workers, w)
	s.mtx.Unlock()
	s.AddView(w.bkt)
	return w
}

// stopWorkers shuts every worker down and waits for them, so nothing is
// uploading when a rollback runs.
func (s *scenarioRun) stopWorkers() {
	s.mtx.Lock()
	ws := s.workers
	s.mtx.Unlock()
	for _, w := range ws {
		w.cancel()
	}
	for _, w := range ws {
		<-w.done
	}
}

func (s *scenarioRun) journal() *Journal {
	s.t.Helper()
	j, err := ReadJournal(context.Background(), s.Shared, s.conf.journalID)
	testutil.Ok(s.t, err)
	if j == nil {
		s.t.Fatal("journal does not exist")
	}
	return j
}

func (s *scenarioRun) tasksInState(state TaskState) []*TaskEntry {
	var out []*TaskEntry
	for _, e := range s.journal().Tasks {
		if e.State == state {
			out = append(out, e)
		}
	}
	return out
}

// waitLeasedBy waits until the worker holds a lease.
func (s *scenarioRun) waitLeasedBy(workerID string) {
	s.t.Helper()
	s.WaitFor(workerID+" to lease a task", func() bool {
		for _, e := range s.tasksInState(StateLeased) {
			if e.Lease != nil && e.Lease.WorkerID == workerID {
				return true
			}
		}
		return false
	})
}

// assertProvenanceIntact checks that every worker-produced block still names
// sources that exist in the bucket, marked or not, so that a rollback could
// still undo it.
func assertProvenanceIntact(t *testing.T, d *compacttest.BucketDump, bkt objstore.Bucket) {
	t.Helper()
	for _, b := range d.Blocks {
		if _, ok := ProvenanceOf(b.Meta); !ok {
			continue
		}
		for _, s := range b.Sources {
			testutil.Assert(t, exists(t, bkt, filepath.Join(s, block.MetaFilename)),
				"source %s of worker-produced block %s is gone from the bucket", s, b.ID)
		}
	}
}

// newGenericRun is the run the shared scenarios are judged on: a manager with
// two workers, both on the deployment's deduplication configuration.
func newGenericRun(t *testing.T, c *compacttest.Corpus, conf compacttest.NodeConfig) *compacttest.Run {
	t.Helper()
	s := newScenarioRun(t, c, nodeConfig{NodeConfig: conf})
	s.startWorker("w1")
	s.startWorker("w2")
	return s.Run
}

// TestGenericScenarios runs the scenarios every compactor has to survive
// against a manager and its workers.
func TestGenericScenarios(t *testing.T) {
	skipUnlessScenarios(t)
	compacttest.RunSuite(t, compacttest.Suite{NewRun: newGenericRun})
}

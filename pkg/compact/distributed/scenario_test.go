// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

// This file is the fault scenario suite for the distributed compactor: the
// whole control loop of the binary - sync, grouping, planning, dispatch,
// verification, garbage collection and both downsampling passes - run in
// process against a synthetic bucket, with faults injected between the pieces,
// and the outcome judged against what the standalone compactor produces from
// the same blocks.
//
// The oracle is content, not blocks. The manager keeps several plans for one
// group in flight, so its intermediate block layout legitimately differs from
// standalone's; what must not differ is the data a store gateway would serve:
// every series and sample at every resolution, per external label set. See
// dumpBucket.
//
// The corpus is synthesized, not scraped: a 2h window of an HA Prometheus pair
// (prometheus_replica as a series label inside the blocks) ingested by receive
// with replication factor 2 (receiver_replica as an external label) is two
// identical blocks per window, each holding both Prometheus replicas' series.
// Offline penalty deduplication collapses the two copies; the Prometheus
// replicas stay separate series, as query-time deduplication expects.
//
// The suite is slow and off by default. Run it with
//
//	go test ./pkg/compact/distributed/ -run TestScenarios -race -args -distributed.scenarios
//
// or with THANOS_DISTRIBUTED_SCENARIOS set in the environment.

var runScenarios = flag.Bool("distributed.scenarios", false,
	"Run the in-process fault scenario suite for the distributed compactor. Slow; off by default.")

func skipUnlessScenarios(t *testing.T) {
	t.Helper()
	if !*runScenarios && os.Getenv("THANOS_DISTRIBUTED_SCENARIOS") == "" {
		t.Skip("scenario suite disabled; run with -distributed.scenarios or THANOS_DISTRIBUTED_SCENARIOS=1")
	}
}

// scnWindow is the raw block length, as Prometheus and receive cut them.
const scnWindow = 2 * time.Hour

// scnLevels are the compaction ranges every node and worker plans with. A raw
// block is downsampled to 5m once it spans 40h, which the third level reaches.
var scnLevels = []int64{scnWindow.Milliseconds(), (8 * time.Hour).Milliseconds(), (48 * time.Hour).Milliseconds()}

// tenantSpec describes one block stream of the corpus.
type tenantSpec struct {
	name string
	// promReplicas are the prometheus_replica values written as a series
	// label inside every block; empty means a single Prometheus.
	promReplicas []string
	// receivers are the receiver_replica values written as an external label,
	// one block per receiver per window; empty means no receive replication.
	receivers []string
	series    int
	windows   int
	samples   int
	// noCompact marks the given windows' blocks no-compact for the reason.
	noCompact map[int]metadata.NoCompactReason
}

type corpusBlock struct {
	id     ulid.ULID
	dir    string
	tenant string
	window int
	mark   metadata.NoCompactReason
}

type corpus struct {
	name   string
	blocks []corpusBlock
}

// haCorpus is the default corpus: the deployment shape the suite exists for,
// plus a small plain tenant so that two groups are always in play.
func haCorpus() []tenantSpec {
	return []tenantSpec{
		// The planner never compacts the newest range, so both streams reach
		// one window past the range that has to be compacted: 48h for the HA
		// stream, which is what 5m downsampling needs, 8h for the plain one.
		{name: "ha", promReplicas: []string{"A", "B"}, receivers: []string{"r0", "r1"}, series: 3, windows: 25, samples: 20},
		{name: "plain", series: 2, windows: 5, samples: 20},
	}
}

func buildCorpus(t *testing.T, name string, tenants []tenantSpec) *corpus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c := &corpus{name: name}
	dir := t.TempDir()
	for _, tn := range tenants {
		receivers := tn.receivers
		if len(receivers) == 0 {
			receivers = []string{""}
		}
		promReplicas := tn.promReplicas
		if len(promReplicas) == 0 {
			promReplicas = []string{""}
		}
		var series []labels.Labels
		for _, pr := range promReplicas {
			for i := range tn.series {
				b := labels.NewBuilder(labels.EmptyLabels())
				b.Set("__name__", fmt.Sprintf("metric_%d", i))
				b.Set("job", tn.name)
				if pr != "" {
					b.Set("prometheus_replica", pr)
				}
				series = append(series, b.Labels())
			}
		}
		for w := range tn.windows {
			mint := int64(w) * scnWindow.Milliseconds()
			maxt := mint + scnWindow.Milliseconds()
			for _, rcv := range receivers {
				b := labels.NewBuilder(labels.EmptyLabels())
				b.Set("tenant", tn.name)
				if rcv != "" {
					b.Set("receiver_replica", rcv)
				}
				id, err := e2eutil.CreateBlock(ctx, dir, series, tn.samples, mint, maxt, b.Labels(), 0, metadata.NoneFunc, nil)
				testutil.Ok(t, err)
				c.blocks = append(c.blocks, corpusBlock{
					id: id, dir: filepath.Join(dir, id.String()), tenant: tn.name, window: w, mark: tn.noCompact[w],
				})
			}
		}
	}
	return c
}

// upload puts the corpus into a bucket, marks included. The block IDs are the
// same in every bucket the corpus is uploaded to, which is what lets a rollback
// be checked against the input exactly.
func (c *corpus) upload(t *testing.T, bkt objstore.Bucket) {
	t.Helper()
	ctx := context.Background()
	logger := log.NewNopLogger()
	for _, b := range c.blocks {
		testutil.Ok(t, block.Upload(ctx, logger, bkt, b.dir, metadata.NoneFunc))
		if b.mark != "" {
			testutil.Ok(t, block.MarkForNoCompact(ctx, logger, bkt, b.id, b.mark, "scenario corpus",
				prometheus.NewCounter(prometheus.CounterOpts{Name: "scenario_marks"})))
		}
	}
}

func (c *corpus) ids() []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(c.blocks))
	for _, b := range c.blocks {
		ids = append(ids, b.id)
	}
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return ids
}

// --- the oracle ---------------------------------------------------------------

type scnSample struct {
	t int64
	v float64
}

// aggrRaw keys the samples of raw blocks; downsampled blocks are keyed by
// aggregate type.
const aggrRaw = downsample.AggrType(math.MaxUint8)

var scnAggrTypes = []downsample.AggrType{downsample.AggrCount, downsample.AggrSum, downsample.AggrMin, downsample.AggrMax, downsample.AggrCounter}

type servedBlock struct {
	id      ulid.ULID
	ext     string
	res     int64
	mint    int64
	maxt    int64
	sources []string
	prov    *Provenance
}

// bucketDump is what a store gateway would serve from a bucket: the blocks
// that carry no deletion mark, deduplicated the way the store's fetcher does,
// and every series and sample in them keyed by resolution, external labels
// and series labels.
type bucketDump struct {
	series map[string]map[downsample.AggrType][]scnSample
	blocks []servedBlock
}

func dumpBucket(t *testing.T, bkt objstore.Bucket) *bucketDump {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	logger := log.NewNopLogger()
	insBkt := objstore.WithNoopInstr(bkt)

	base, err := block.NewBaseFetcher(logger, 4, insBkt, block.NewConcurrentLister(logger, insBkt), "", prometheus.NewRegistry())
	testutil.Ok(t, err)
	fetcher := base.NewMetaFetcher(prometheus.NewRegistry(), []block.MetadataFilter{
		block.NewIgnoreDeletionMarkFilter(logger, insBkt, 0, 4),
		block.NewDeduplicateFilter(4),
	})
	metas, _, err := fetcher.Fetch(ctx)
	testutil.Ok(t, err)

	d := &bucketDump{series: map[string]map[downsample.AggrType][]scnSample{}}
	dir := t.TempDir()
	for id, m := range metas {
		sb := servedBlock{id: id, ext: labels.FromMap(m.Thanos.Labels).String(), res: m.Thanos.Downsample.Resolution, mint: m.MinTime, maxt: m.MaxTime}
		for _, s := range m.Compaction.Sources {
			sb.sources = append(sb.sources, s.String())
		}
		slices.Sort(sb.sources)
		if p, ok := ProvenanceOf(m); ok {
			sb.prov = &p
		}
		d.blocks = append(d.blocks, sb)
		d.readBlock(t, ctx, bkt, dir, m)
	}
	slices.SortFunc(d.blocks, func(a, b servedBlock) int { return a.id.Compare(b.id) })
	for _, byAggr := range d.series {
		for _, samples := range byAggr {
			slices.SortFunc(samples, func(a, b scnSample) int {
				if a.t != b.t {
					return int(a.t - b.t)
				}
				switch {
				case a.v < b.v:
					return -1
				case a.v > b.v:
					return 1
				}
				return 0
			})
		}
	}
	return d
}

func (d *bucketDump) readBlock(t *testing.T, ctx context.Context, bkt objstore.Bucket, dir string, m *metadata.Meta) {
	t.Helper()
	logger := log.NewNopLogger()
	bdir := filepath.Join(dir, m.ULID.String())
	testutil.Ok(t, block.Download(ctx, logger, bkt, m.ULID, bdir))
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), bdir, downsample.NewPool(), nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, b.Close()) }()

	indexr, err := b.Index()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, indexr.Close()) }()
	chunkr, err := b.Chunks()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, chunkr.Close()) }()

	k, v := index.AllPostingsKey()
	all, err := indexr.Postings(ctx, k, v)
	testutil.Ok(t, err)
	ext := labels.FromMap(m.Thanos.Labels).String()
	for all.Next() {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta
		testutil.Ok(t, indexr.Series(all.At(), &builder, &chks))
		key := fmt.Sprintf("res=%d ext=%s series=%s", m.Thanos.Downsample.Resolution, ext, builder.Labels().String())
		byAggr := d.series[key]
		if byAggr == nil {
			byAggr = map[downsample.AggrType][]scnSample{}
			d.series[key] = byAggr
		}
		for _, c := range chks {
			chk, _, err := chunkr.ChunkOrIterable(c)
			testutil.Ok(t, err)
			if m.Thanos.Downsample.Resolution == 0 {
				byAggr[aggrRaw] = append(byAggr[aggrRaw], floatSamples(t, chk)...)
				continue
			}
			ac, ok := chk.(*downsample.AggrChunk)
			testutil.Assert(t, ok, "block %s at resolution %d holds a %T, not an aggregate chunk", m.ULID, m.Thanos.Downsample.Resolution, chk)
			for _, at := range scnAggrTypes {
				sub, err := ac.Get(at)
				if err != nil {
					continue // Not every aggregate is present for every series.
				}
				byAggr[at] = append(byAggr[at], floatSamples(t, sub)...)
			}
		}
	}
	testutil.Ok(t, all.Err())
}

func floatSamples(t *testing.T, chk chunkenc.Chunk) []scnSample {
	t.Helper()
	var out []scnSample
	it := chk.Iterator(nil)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		testutil.Equals(t, chunkenc.ValFloat, vt, "the corpus holds floats only")
		ts, v := it.At()
		out = append(out, scnSample{t: ts, v: v})
	}
	testutil.Ok(t, it.Err())
	return out
}

// assertSameContent fails with the first differences between two dumps.
func assertSameContent(t *testing.T, want, got *bucketDump, what string) {
	t.Helper()
	if !sameContent(t, want, got, what) {
		t.FailNow()
	}
}

// sameContent reports the first differences between two dumps and whether
// there were any.
func sameContent(t *testing.T, want, got *bucketDump, what string) bool {
	t.Helper()
	keys := map[string]struct{}{}
	for k := range want.series {
		keys[k] = struct{}{}
	}
	for k := range got.series {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var diffs []string
	for _, k := range sorted {
		w, g := want.series[k], got.series[k]
		switch {
		case w == nil:
			diffs = append(diffs, fmt.Sprintf("unexpected %s", k))
			continue
		case g == nil:
			diffs = append(diffs, fmt.Sprintf("missing %s", k))
			continue
		}
		for _, at := range append([]downsample.AggrType{aggrRaw}, scnAggrTypes...) {
			ws, gs := w[at], g[at]
			if len(ws) != len(gs) {
				diffs = append(diffs, fmt.Sprintf("%s aggr=%d: want %d samples, got %d", k, at, len(ws), len(gs)))
				continue
			}
			for i := range ws {
				if ws[i] != gs[i] {
					diffs = append(diffs, fmt.Sprintf("%s aggr=%d: sample %d want %+v, got %+v", k, at, i, ws[i], gs[i]))
					break
				}
			}
		}
	}
	if len(diffs) > 0 {
		if len(diffs) > 12 {
			diffs = append(diffs[:12], fmt.Sprintf("... %d more", len(diffs)-12))
		}
		t.Logf("expected layout:\n  %s\ngot layout:\n  %s", strings.Join(want.layout(), "\n  "), strings.Join(got.layout(), "\n  "))
		t.Errorf("%s: bucket content differs:\n  %s", what, strings.Join(diffs, "\n  "))
		return false
	}
	if len(got.series) == 0 {
		t.Errorf("%s: the dump is empty, the oracle proves nothing", what)
		return false
	}
	return true
}

// layout describes the served blocks by what they are made of, not by ID.
func (d *bucketDump) layout() []string {
	var out []string
	for _, b := range d.blocks {
		out = append(out, fmt.Sprintf("res=%d ext=%s [%d,%d) sources=%v", b.res, b.ext, b.mint, b.maxt, b.sources))
	}
	slices.Sort(out)
	return out
}

func (d *bucketDump) servedIDs() []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(d.blocks))
	for _, b := range d.blocks {
		ids = append(ids, b.id)
	}
	return ids
}

// assertNoOverlaps checks that within one external label set and resolution
// the served blocks do not overlap in time, which is what a double execution
// or a bracketing plan would produce.
func (d *bucketDump) assertNoOverlaps(t *testing.T) {
	t.Helper()
	groups := map[string][]servedBlock{}
	for _, b := range d.blocks {
		k := fmt.Sprintf("%s/%d", b.ext, b.res)
		groups[k] = append(groups[k], b)
	}
	for k, bs := range groups {
		slices.SortFunc(bs, func(a, b servedBlock) int { return int(a.mint - b.mint) })
		for i := 1; i < len(bs); i++ {
			if bs[i].mint < bs[i-1].maxt {
				t.Errorf("group %s: served blocks %s [%d,%d) and %s [%d,%d) overlap", k,
					bs[i-1].id, bs[i-1].mint, bs[i-1].maxt, bs[i].id, bs[i].mint, bs[i].maxt)
			}
		}
	}
}

// assertProvenanceIntact checks that every worker-produced block still names
// sources that exist in the bucket, marked or not, so that a rollback could
// still undo it.
func (d *bucketDump) assertProvenanceIntact(t *testing.T, bkt objstore.Bucket) {
	t.Helper()
	for _, b := range d.blocks {
		if b.prov == nil {
			continue
		}
		for _, s := range b.sources {
			testutil.Assert(t, exists(t, bkt, filepath.Join(s, block.MetaFilename)),
				"source %s of worker-produced block %s is gone from the bucket", s, b.id)
		}
	}
}

func assertNoDeletionMarks(t *testing.T, bkt objstore.Bucket) {
	t.Helper()
	testutil.Ok(t, bkt.Iter(context.Background(), "", func(name string) error {
		if strings.HasSuffix(name, metadata.DeletionMarkFilename) {
			t.Errorf("unexpected deletion mark %s", name)
		}
		return nil
	}, objstore.WithRecursiveIter()))
}

func blockIDs(t *testing.T, bkt objstore.Bucket) []ulid.ULID {
	t.Helper()
	var ids []ulid.ULID
	testutil.Ok(t, bkt.Iter(context.Background(), "", func(name string) error {
		if !strings.HasSuffix(name, "/"+block.MetaFilename) {
			return nil
		}
		id, err := ulid.Parse(strings.TrimSuffix(name, "/"+block.MetaFilename))
		if err == nil {
			ids = append(ids, id)
		}
		return nil
	}, objstore.WithRecursiveIter()))
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return ids
}

// --- one compactor process ------------------------------------------------------

const (
	modeStandalone = "standalone"
	modeManager    = "manager"
)

type nodeConfig struct {
	mode               string
	journalID          string
	dedupReplicaLabels []string
	dedupFunc          string
	leaseTTL           time.Duration
	maxAttempts        int
	maxInflight        int
	maxTaskSeries      uint64
	deleteDelay        time.Duration
}

func (c nodeConfig) withDefaults() nodeConfig {
	if c.mode == "" {
		c.mode = modeManager
	}
	if c.journalID == "" {
		c.journalID = "scenario-shard"
	}
	if c.leaseTTL == 0 {
		c.leaseTTL = 250 * time.Millisecond
	}
	if c.maxAttempts == 0 {
		c.maxAttempts = 3
	}
	if c.maxInflight == 0 {
		c.maxInflight = 4
	}
	if c.deleteDelay == 0 {
		// Long enough that nothing is physically deleted during a scenario,
		// which is what a rollback relies on and what --delete-delay is for.
		c.deleteDelay = 48 * time.Hour
	}
	return c
}

// haNodeConfig is the manager configuration of the deployment the suite
// exists for: penalty deduplication over the receive replica label.
func haNodeConfig() nodeConfig {
	return nodeConfig{dedupReplicaLabels: []string{"receiver_replica"}, dedupFunc: compact.DedupAlgorithmPenalty}
}

// node is one compactor process, standalone or manager, wired exactly as
// cmd/thanos/compact.go wires it, over its own fault-injectable view of the
// shared bucket.
type node struct {
	t      *testing.T
	logger log.Logger
	conf   nodeConfig
	bkt    *hookBucket
	dir    string

	sy                 *compact.Syncer
	compactor          *compact.BucketCompactor
	noCompactFilter    *compact.GatherNoCompactionMarkFilter
	noDownsampleFilter *downsample.GatherNoDownsampleMarkFilter
	sched              *Scheduler
	downsamples        *prometheus.CounterVec
	downsampleFailures *prometheus.CounterVec

	// halted is set when the maintenance loop found a halt condition, which
	// ends the binary's process.
	halted atomic.Bool

	// ctx is the node's lifetime; stop ends it, and with it any pass.
	ctx  context.Context
	stop context.CancelFunc

	mtx        sync.Mutex
	iterCancel context.CancelFunc
}

func mergeFuncFor(dedupFunc string) storage.VerticalChunkSeriesMergeFunc {
	if dedupFunc == compact.DedupAlgorithmPenalty {
		return dedup.NewChunkSeriesMerger()
	}
	return storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)
}

func newNode(t *testing.T, shared objstore.Bucket, handler *switchableHandler, conf nodeConfig) *node {
	t.Helper()
	conf = conf.withDefaults()
	n := &node{
		t:      t,
		logger: log.NewNopLogger(),
		conf:   conf,
		bkt:    &hookBucket{Bucket: shared},
		dir:    t.TempDir(),
	}
	reg := prometheus.NewRegistry()
	insBkt := objstore.WithNoopInstr(n.bkt)
	counter := func() prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "scenario_counter"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.ctx, n.stop = ctx, cancel
	t.Cleanup(cancel)

	vertical := len(conf.dedupReplicaLabels) > 0

	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(n.logger, insBkt, conf.deleteDelay/2, 4)
	duplicateBlocksFilter := block.NewDeduplicateFilter(4)
	n.noCompactFilter = compact.NewGatherNoCompactionMarkFilter(n.logger, insBkt, 4)
	n.noDownsampleFilter = downsample.NewGatherNoDownsampleMarkFilter(n.logger, insBkt, 4)

	base, err := block.NewBaseFetcher(n.logger, 4, insBkt, block.NewConcurrentLister(n.logger, insBkt), "", extprom.WrapRegistererWithPrefix("thanos_", reg))
	testutil.Ok(t, err)
	fetcher := base.NewMetaFetcher(extprom.WrapRegistererWithPrefix("thanos_", reg), []block.MetadataFilter{
		ignoreDeletionMarkFilter,
		block.NewReplicaLabelRemover(n.logger, conf.dedupReplicaLabels),
		duplicateBlocksFilter,
		n.noCompactFilter,
		n.noDownsampleFilter,
	})
	n.sy, err = compact.NewMetaSyncer(n.logger, reg, insBkt, fetcher, duplicateBlocksFilter, ignoreDeletionMarkFilter, counter(), counter(), 0)
	testutil.Ok(t, err)

	comp, err := tsdb.NewLeveledCompactor(ctx, reg, logutil.GoKitLogToSlog(n.logger), scnLevels, downsample.NewPool(), mergeFuncFor(conf.dedupFunc))
	testutil.Ok(t, err)

	grouper := compact.NewDefaultGrouper(n.logger, insBkt, false, vertical, reg, counter(), counter(), counter(), metadata.NoneFunc, 1, 1)
	tsdbPlanner := compact.NewPlanner(n.logger, scnLevels, n.noCompactFilter)
	largeIndexPlanner := compact.WithLargeTotalIndexSizeFilter(tsdbPlanner, insBkt, math.MaxInt64, counter())
	var planner compact.Planner = largeIndexPlanner
	if vertical {
		planner = compact.WithVerticalCompactionDownsampleFilter(largeIndexPlanner, insBkt, counter())
	}
	cleaner := compact.NewBlocksCleaner(n.logger, insBkt, ignoreDeletionMarkFilter, conf.deleteDelay, counter(), counter())

	var executor compact.PlanExecutor = compact.LocalPlanExecutor{
		Comp:                   comp,
		BlockDeletableChecker:  compact.DefaultBlockDeletableChecker{},
		Callback:               compact.DefaultCompactionLifecycleCallback{},
		MarkSourcesForDeletion: true,
	}
	if conf.mode == modeManager {
		n.sched, err = NewScheduler(ctx, n.logger, n.bkt, reg, ManagerConfig{
			JournalID:          conf.journalID,
			DedupFunc:          conf.dedupFunc,
			DedupReplicaLabels: conf.dedupReplicaLabels,
			LeaseTTL:           conf.leaseTTL,
			MaxAttempts:        conf.maxAttempts,
			MaxTaskSeries:      conf.maxTaskSeries,
		})
		testutil.Ok(t, err)
		executor = NewRemotePlanExecutor(n.logger, n.bkt, n.sched, planner, conf.maxInflight)

		mux := http.NewServeMux()
		RegisterServer(mux, n.logger, n.sched)
		handler.swap(mux)

		// The maintenance loop of the binary: expire leases, prune, unpark.
		// A halt found here ends the manager, as it ends the process.
		go func() {
			_ = runutil.Repeat(conf.leaseTTL/4, ctx.Done(), func() error {
				err := n.sched.Maintain()
				if err == nil {
					return nil
				}
				if !compact.IsHaltError(err) {
					level.Warn(n.logger).Log("msg", "maintenance", "err", err)
					return nil
				}
				n.halted.Store(true)
				n.abortIteration()
				return err
			})
		}()
	}

	n.compactor, err = compact.NewBucketCompactorWithExecutor(n.logger, n.sy, grouper, planner, executor,
		filepath.Join(n.dir, "compact"), insBkt, 2, false, cleaner)
	testutil.Ok(t, err)

	n.downsamples = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scenario_downsamples"}, []string{"resolution"})
	n.downsampleFailures = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scenario_downsample_failures"}, []string{"resolution"})
	return n
}

// iterate is one pass of the binary's main function: compact until nothing is
// left, then two downsampling passes so the second can pick up what the first
// produced.
func (n *node) iterate(ctx context.Context) error {
	if n.ctx.Err() != nil {
		return errors.New("the manager is stopped")
	}
	ctx, cancel := context.WithCancel(ctx)
	context.AfterFunc(n.ctx, cancel)
	n.mtx.Lock()
	n.iterCancel = cancel
	n.mtx.Unlock()
	defer cancel()

	if err := n.compactor.Compact(ctx); err != nil {
		return errors.Wrap(err, "compaction")
	}
	for pass := range 2 {
		if err := n.sy.SyncMetas(ctx); err != nil {
			return errors.Wrapf(err, "sync before downsampling pass %d", pass)
		}
		if err := n.downsample(ctx, n.sy.Metas(), n.noCompactFilter.NoCompactMarkedBlocks(), n.noDownsampleFilter.NoDownsampleMarkedBlocks()); err != nil {
			return errors.Wrapf(err, "downsampling pass %d", pass)
		}
	}
	return nil
}

// abortIteration cancels the pass in progress, as killing the process would.
func (n *node) abortIteration() {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if n.iterCancel != nil {
		n.iterCancel()
	}
}

// downsampleLocally is the standalone downsampler's processDownsampling.
func (n *node) downsampleLocally(ctx context.Context, m *metadata.Meta, resolution int64) error {
	dir := filepath.Join(n.dir, "downsample")
	bdir := filepath.Join(dir, m.ULID.String())
	if err := block.Download(ctx, n.logger, n.bkt, m.ULID, bdir); err != nil {
		return compact.NewRetryError(errors.Wrapf(err, "download block %s", m.ULID))
	}
	pool := chunkenc.Pool(chunkenc.NewPool())
	if m.Thanos.Downsample.Resolution != 0 {
		pool = downsample.NewPool()
	}
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(n.logger), bdir, pool, nil)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	id, err := downsample.Downsample(ctx, n.logger, m, b, dir, resolution)
	runutil.CloseWithLogOnErr(n.logger, b, "tsdb reader")
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())
	if err := block.Upload(ctx, n.logger, n.bkt, resdir, metadata.NoneFunc); err != nil {
		return compact.NewRetryError(errors.Wrapf(err, "upload downsampled block %s", id))
	}
	_ = os.RemoveAll(bdir)
	_ = os.RemoveAll(resdir)
	return nil
}

// --- a scenario run ---------------------------------------------------------------

// scenarioRun is one bucket, one manager behind a stable URL, and the workers
// a scenario starts against it.
type scenarioRun struct {
	t       *testing.T
	shared  objstore.Bucket
	handler *switchableHandler
	srv     *httptest.Server
	conf    nodeConfig
	corpus  *corpus

	mtx     sync.Mutex
	manager *node
	workers []*scnWorker
	// background tracks the scenario's fault-injection goroutines, so that a
	// scenario never ends with one still running.
	background sync.WaitGroup
}

// inject runs a fault-injection routine in the background; the test waits for
// it to end before it finishes.
func (s *scenarioRun) inject(f func()) {
	s.background.Go(f)
}

// wait blocks until every fault-injection routine has ended.
func (s *scenarioRun) wait() {
	s.background.Wait()
}

// passUntil runs single passes of the current manager until cond holds,
// for a control loop that is not expected to settle, such as one whose
// group fails on every pass.
func (s *scenarioRun) passUntil(msg string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for i := 1; !cond(); i++ {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %s", msg)
		}
		n := s.currentManager()
		if err := n.iterate(context.Background()); err != nil {
			testutil.Assert(s.t, !compact.IsHaltError(err) && !n.halted.Load(), "the manager halted: %v", err)
			s.t.Logf("pass %d: %v", i, err)
		}
	}
}

func newScenarioRun(t *testing.T, c *corpus, conf nodeConfig) *scenarioRun {
	t.Helper()
	s := &scenarioRun{
		t:       t,
		shared:  objstore.NewInMemBucket(),
		handler: &switchableHandler{},
		conf:    conf.withDefaults(),
	}
	s.handler.swap(http.NotFoundHandler())
	s.srv = httptest.NewServer(s.handler)
	t.Cleanup(s.srv.Close)
	c.upload(t, s.shared)
	s.manager = newNode(t, s.shared, s.handler, s.conf)
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
	old := s.currentManager()
	n := newNode(s.t, s.shared, s.handler, conf)
	s.mtx.Lock()
	s.manager = n
	s.mtx.Unlock()
	// The successor is in place before the old manager dies, so the
	// convergence loop carries on with it.
	old.abortIteration()
	old.stop()
	return n
}

type workerOpts struct {
	journalID          string
	dedupFunc          string
	dedupReplicaLabels []string
}

// startWorker runs a real worker against the manager URL with the manager's
// deduplication configuration, unless the options say otherwise.

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

func (s *scenarioRun) startWorker(id string, opts ...workerOpts) *scnWorker {
	s.t.Helper()
	o := workerOpts{journalID: s.conf.journalID, dedupFunc: s.conf.dedupFunc, dedupReplicaLabels: s.conf.dedupReplicaLabels}
	if len(opts) > 0 {
		o = opts[0]
	}
	w := &scnWorker{
		testWorker: &testWorker{
			id:   id,
			bkt:  &hookBucket{Bucket: s.shared},
			reg:  prometheus.NewRegistry(),
			done: make(chan struct{}),
		},
		dead: &atomic.Bool{},
	}
	logger := log.NewNopLogger()
	comp, err := tsdb.NewLeveledCompactor(context.Background(), w.reg, logutil.GoKitLogToSlog(logger), scnLevels, downsample.NewPool(), mergeFuncFor(o.dedupFunc))
	testutil.Ok(s.t, err)

	client := NewHTTPClient(logger, dns.NewProvider(logger, prometheus.NewRegistry(), dns.GolangResolverType),
		strings.TrimPrefix(s.srv.URL, "http://"), 5*time.Second)
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
	j, err := ReadJournal(context.Background(), s.shared, s.conf.journalID)
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

// waitFor polls until cond holds.
func (s *scenarioRun) waitFor(msg string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %s", msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitLeasedBy waits until the worker holds a lease and returns the task.
func (s *scenarioRun) waitLeasedBy(workerID string) {
	s.t.Helper()
	s.waitFor(workerID+" to lease a task", func() bool {
		for _, e := range s.tasksInState(StateLeased) {
			if e.Lease != nil && e.Lease.WorkerID == workerID {
				return true
			}
		}
		return false
	})
}

// fingerprint summarizes the bucket: every object name, and the content of
// every metadata and marker file. Two identical fingerprints around a pass
// mean the pass changed nothing.
func (s *scenarioRun) fingerprint() string {
	h := sha256.New()
	ctx := context.Background()
	_ = s.shared.Iter(ctx, "", func(name string) error {
		_, _ = h.Write([]byte(name))
		if strings.HasSuffix(name, ".json") {
			rc, err := s.shared.Get(ctx, name)
			if err != nil {
				return nil
			}
			defer rc.Close()
			buf := make([]byte, 64<<10)
			for {
				k, err := rc.Read(buf)
				_, _ = h.Write(buf[:k])
				if err != nil {
					break
				}
			}
		}
		return nil
	}, objstore.WithRecursiveIter())
	return hex.EncodeToString(h.Sum(nil))
}

type convergeResult struct {
	halted     bool
	iterations int
	lastErr    error
}

// converge runs passes of the current manager until two consecutive passes
// changed nothing and the journal holds no live task, the way the binary's
// wait loop would keep going. Retryable and canceled passes are logged and
// retried; a halt ends the run, as it would end the binary's.
func (s *scenarioRun) converge(timeout time.Duration) convergeResult {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	stable := 0
	res := convergeResult{}
	for {
		n := s.currentManager()
		err := n.iterate(context.Background())
		res.iterations++
		res.lastErr = err
		if err != nil && compact.IsHaltError(err) || n.halted.Load() {
			res.halted = true
			return res
		}
		if n.ctx.Err() != nil && s.currentManager() == n {
			// Stopped from outside with no successor, as a scenario killing
			// the manager does. A replaced manager's successor carries on.
			return res
		}
		if err != nil {
			s.t.Logf("pass %d: %v", res.iterations, err)
		}
		quiet := n.sched == nil || len(s.tasksInState(StatePending))+len(s.tasksInState(StateLeased)) == 0
		fp := s.fingerprint()
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
			s.t.Fatalf("the bucket did not converge within %s after %d passes; last error: %v", timeout, res.iterations, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// golden runs the standalone compactor over the corpus to convergence and
// returns what it left behind.
func golden(t *testing.T, c *corpus, conf nodeConfig) *bucketDump {
	t.Helper()
	conf = conf.withDefaults()
	conf.mode = modeStandalone
	bkt := objstore.NewInMemBucket()
	c.upload(t, bkt)
	n := newNode(t, bkt, &switchableHandler{}, conf)
	s := &scenarioRun{t: t, shared: bkt, conf: conf, manager: n}
	res := s.converge(2 * time.Minute)
	testutil.Assert(t, !res.halted, "the standalone compactor halted on the corpus: %v", res.lastErr)
	d := dumpBucket(t, bkt)
	t.Logf("standalone layout after %d passes:\n  %s", res.iterations, strings.Join(d.layout(), "\n  "))
	return d
}

// inputDump is the corpus as uploaded, before any compactor touched it.
func inputDump(t *testing.T, c *corpus) *bucketDump {
	t.Helper()
	bkt := objstore.NewInMemBucket()
	c.upload(t, bkt)
	return dumpBucket(t, bkt)
}

func counterTotal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	testutil.Ok(t, err)
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

func (b *hookBucket) setOnUpload(f func(ctx context.Context, name string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.onUpload = f
}

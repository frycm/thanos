// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import (
	"context"
	"maps"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// NodeConfig is the configuration of one compactor process that every
// compactor, whatever executes its plans, shares.
type NodeConfig struct {
	DedupReplicaLabels []string
	DedupFunc          string
	DeleteDelay        time.Duration
	Levels             []int64
	// Split is --compact.block-split.*; the zero value never splits.
	Split compact.SplitConfig
	// MaxIndexSize is --compact.block-max-index-size; zero means unlimited.
	MaxIndexSize         int64
	Concurrency          int
	AcceptMalformedIndex bool
}

// WithDefaults fills the zero values.
func (c NodeConfig) WithDefaults() NodeConfig {
	if c.DeleteDelay == 0 {
		// Long enough that nothing is physically deleted during a scenario,
		// which is what a rollback relies on and what --delete-delay is for.
		c.DeleteDelay = 48 * time.Hour
	}
	if len(c.Levels) == 0 {
		c.Levels = Levels
	}
	if c.Concurrency == 0 {
		c.Concurrency = 2
	}
	return c
}

// Vertical reports whether the configuration enables vertical compaction.
func (c NodeConfig) Vertical() bool { return len(c.DedupReplicaLabels) > 0 }

// HANodeConfig is the configuration of the deployment the suites exist for:
// penalty deduplication over the Prometheus and receive replica labels.
func HANodeConfig() NodeConfig {
	return NodeConfig{DedupReplicaLabels: []string{"prometheus_replica", "receiver_replica", "otelcol_replica", "ruler_replica"}, DedupFunc: compact.DedupAlgorithmPenalty}
}

// MergeFuncFor returns the vertical merge function for a deduplication function name.
func MergeFuncFor(dedupFunc string) storage.VerticalChunkSeriesMergeFunc {
	if dedupFunc == compact.DedupAlgorithmPenalty {
		return dedup.NewChunkSeriesMerger()
	}
	return storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)
}

// Hooks let a feature replace the parts of a node it changes. Nil hooks keep
// the standalone compactor's behavior.
type Hooks struct {
	// Executor builds the executor that runs the node's compaction plans. The
	// default executes them in process.
	Executor func(n *Node, planner compact.Planner) compact.PlanExecutor
	// Downsample runs one downsampling pass over the synced metadata. The
	// default downsamples in process what downsample.Plan selects.
	Downsample func(ctx context.Context, n *Node, metas map[ulid.ULID]*metadata.Meta, noCompact map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error
}

// Node is one compactor process wired exactly as cmd/thanos/compact.go wires
// it, over its own fault-injectable view of a shared bucket.
type Node struct {
	T      *testing.T
	Logger log.Logger
	Conf   NodeConfig
	Bkt    *HookBucket
	Dir    string
	Reg    *prometheus.Registry

	Syncer             *compact.Syncer
	Compactor          *compact.BucketCompactor
	DedupFilter        *block.DefaultDeduplicateFilter
	NoCompactFilter    *compact.GatherNoCompactionMarkFilter
	NoDownsampleFilter *downsample.GatherNoDownsampleMarkFilter

	// Halted is set by whoever finds a condition that ends the process
	// outside a pass, such as a manager's maintenance loop.
	Halted atomic.Bool

	// Ctx is the node's lifetime; Stop ends it, and with it any pass.
	Ctx  context.Context
	stop context.CancelFunc

	downsample func(ctx context.Context, n *Node, metas map[ulid.ULID]*metadata.Meta, noCompact map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error

	mtx        sync.Mutex
	iterCancel context.CancelFunc
}

func counter() prometheus.Counter {
	return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "scenario_events_total", Help: "Events the compactor under test counts; unregistered."})
}

// NewNode builds a node over the shared bucket.
func NewNode(t *testing.T, shared objstore.Bucket, conf NodeConfig, hooks Hooks) *Node {
	t.Helper()
	conf = conf.WithDefaults()
	logger := log.NewNopLogger()
	if os.Getenv("COMPACTTEST_DEBUG") != "" {
		logger = log.With(log.NewLogfmtLogger(os.Stderr), "node", t.Name())
	}
	n := &Node{
		T:      t,
		Logger: logger,
		Conf:   conf,
		Bkt:    NewHookBucket(shared),
		Dir:    t.TempDir(),
		Reg:    prometheus.NewRegistry(),
	}
	insBkt := objstore.WithNoopInstr(n.Bkt)
	ctx, cancel := context.WithCancel(context.Background())
	n.Ctx, n.stop = ctx, cancel
	t.Cleanup(cancel)

	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(n.Logger, insBkt, conf.DeleteDelay/2, 4)
	duplicateBlocksFilter := block.NewDeduplicateFilter(4)
	duplicateBlocksFilter.HideUnpublished()
	n.DedupFilter = duplicateBlocksFilter
	n.NoCompactFilter = compact.NewGatherNoCompactionMarkFilter(n.Logger, insBkt, 4)
	n.NoDownsampleFilter = downsample.NewGatherNoDownsampleMarkFilter(n.Logger, insBkt, 4)

	base, err := block.NewBaseFetcher(n.Logger, 4, insBkt, block.NewConcurrentLister(n.Logger, insBkt), "", extprom.WrapRegistererWithPrefix("thanos_", n.Reg))
	testutil.Ok(t, err)
	fetcher := base.NewMetaFetcher(extprom.WrapRegistererWithPrefix("thanos_", n.Reg), []block.MetadataFilter{
		ignoreDeletionMarkFilter,
		block.NewReplicaLabelRemover(n.Logger, conf.DedupReplicaLabels),
		duplicateBlocksFilter,
		n.NoCompactFilter,
		n.NoDownsampleFilter,
	})
	n.Syncer, err = compact.NewMetaSyncer(n.Logger, n.Reg, insBkt, fetcher, duplicateBlocksFilter, ignoreDeletionMarkFilter, counter(), counter(), 0)
	testutil.Ok(t, err)

	comp, err := tsdb.NewLeveledCompactor(ctx, n.Reg, logutil.GoKitLogToSlog(n.Logger), conf.Levels, downsample.NewPool(), MergeFuncFor(conf.DedupFunc))
	testutil.Ok(t, err)

	grouper := compact.NewDefaultGrouper(n.Logger, insBkt, conf.AcceptMalformedIndex, conf.Vertical(), n.Reg, counter(), counter(), counter(), metadata.NoneFunc, 1, 1)
	tsdbPlanner := compact.NewPlanner(n.Logger, conf.Levels, n.NoCompactFilter)
	if conf.Split.Enabled() {
		tsdbPlanner = tsdbPlanner.WithStreamNewestAcrossShards(n.Syncer.Metas)
	}
	indexLimit := conf.MaxIndexSize
	if indexLimit <= 0 {
		indexLimit = math.MaxInt64
	}
	largeIndexPlanner := compact.WithLargeTotalIndexSizeFilter(tsdbPlanner, insBkt, conf.Split.PlannerIndexSizeLimit(indexLimit), counter())
	var planner compact.Planner = largeIndexPlanner
	if conf.Vertical() {
		planner = compact.WithVerticalCompactionDownsampleFilter(largeIndexPlanner, insBkt, counter())
	}
	if conf.Split.Enabled() {
		split := conf.Split
		if split.MaxIndexSizeBytes == 0 {
			split.MaxIndexSizeBytes = conf.MaxIndexSize
		}
		planner = compact.WithBlockSplitting(planner, n.Logger, split, nil, insBkt, counter(), n.Syncer.Metas, n.NoCompactFilter.NoCompactMarkedBlocks)
	}
	cleaner := compact.NewBlocksCleaner(n.Logger, insBkt, ignoreDeletionMarkFilter, conf.DeleteDelay, counter(), counter())

	var executor compact.PlanExecutor = compact.LocalPlanExecutor{
		Comp:                   comp,
		BlockDeletableChecker:  compact.DefaultBlockDeletableChecker{},
		Callback:               compact.DefaultCompactionLifecycleCallback{},
		MarkSourcesForDeletion: true,
	}
	if hooks.Executor != nil {
		executor = hooks.Executor(n, planner)
	}
	n.downsample = hooks.Downsample
	if n.downsample == nil {
		n.downsample = downsampleInProcess
	}

	n.Compactor, err = compact.NewBucketCompactorWithExecutor(n.Logger, n.Syncer, grouper, planner, executor,
		filepath.Join(n.Dir, "compact"), insBkt, conf.Concurrency, false, cleaner)
	testutil.Ok(t, err)
	return n
}

// Iterate is one pass of the binary's main function: compact until nothing is
// left, then two downsampling passes so the second can pick up what the first
// produced.
func (n *Node) Iterate(ctx context.Context) error {
	if n.Ctx.Err() != nil {
		return errors.New("the node is stopped")
	}
	ctx, cancel := context.WithCancel(ctx)
	context.AfterFunc(n.Ctx, cancel)
	n.mtx.Lock()
	n.iterCancel = cancel
	n.mtx.Unlock()
	defer cancel()

	if err := n.Compactor.Compact(ctx); err != nil {
		return errors.Wrap(err, "compaction")
	}
	for pass := range 2 {
		if err := n.Syncer.SyncMetas(ctx); err != nil {
			return errors.Wrapf(err, "sync before downsampling pass %d", pass)
		}
		if err := n.downsample(ctx, n, n.Syncer.Metas(), n.NoCompactFilter.NoCompactMarkedBlocks(), n.NoDownsampleFilter.NoDownsampleMarkedBlocks()); err != nil {
			return errors.Wrapf(err, "downsampling pass %d", pass)
		}
	}
	return nil
}

// AbortIteration cancels the pass in progress, as killing the process would.
func (n *Node) AbortIteration() {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if n.iterCancel != nil {
		n.iterCancel()
	}
}

// Stop ends the node for good.
func (n *Node) Stop() { n.stop() }

// Stopped reports whether the node was stopped.
func (n *Node) Stopped() bool { return n.Ctx.Err() != nil }

// downsampleInProcess is the binary's standalone downsampling pass: blocks
// marked no-downsample are taken out of the view, and what downsample.Plan
// selects is downsampled here.
func downsampleInProcess(ctx context.Context, n *Node, metas map[ulid.ULID]*metadata.Meta, _ map[ulid.ULID]*metadata.NoCompactMark, noDownsample map[ulid.ULID]*metadata.NoDownsampleMark) error {
	metas = maps.Clone(metas)
	for id := range noDownsample {
		delete(metas, id)
	}
	candidates, err := downsample.Plan(metas)
	if err != nil {
		return err
	}
	for _, c := range candidates {
		if err := n.DownsampleLocally(ctx, c.Meta, c.TargetResolution); err != nil {
			return err
		}
	}
	return nil
}

// DownsampleLocally is the standalone downsampler's processDownsampling.
func (n *Node) DownsampleLocally(ctx context.Context, m *metadata.Meta, resolution int64) error {
	dir := filepath.Join(n.Dir, "downsample")
	bdir := filepath.Join(dir, m.ULID.String())
	if err := block.Download(ctx, n.Logger, n.Bkt, m.ULID, bdir); err != nil {
		return compact.NewRetryError(errors.Wrapf(err, "download block %s", m.ULID))
	}
	pool := chunkenc.Pool(chunkenc.NewPool())
	if m.Thanos.Downsample.Resolution != 0 {
		pool = downsample.NewPool()
	}
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(n.Logger), bdir, pool, nil)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	id, err := downsample.Downsample(ctx, n.Logger, m, b, dir, resolution)
	runutil.CloseWithLogOnErr(n.Logger, b, "tsdb reader")
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())
	if err := block.Upload(ctx, n.Logger, n.Bkt, resdir, metadata.NoneFunc); err != nil {
		return compact.NewRetryError(errors.Wrapf(err, "upload downsampled block %s", id))
	}
	_ = os.RemoveAll(bdir)
	_ = os.RemoveAll(resdir)
	return nil
}

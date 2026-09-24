// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"path/filepath"
	"slices"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// indexHeadroom is the share of the index size limit a shard may use, leaving
// the rest for index compaction bloat. It matches largeTotalIndexSizeFilter.
const indexHeadroom = 0.85

// SplitConfig says when a compaction is split into several output blocks by
// series, and into how many at most.
type SplitConfig struct {
	// MaxShards caps the shards one plan may be split into. Zero or one
	// disables splitting. It is rounded down to a power of two.
	MaxShards int
	// MaxIndexSizeBytes is the index size a shard aims to stay under, with
	// the same headroom the planner's index size filter applies. Zero
	// disables the index size criterion.
	MaxIndexSizeBytes int64
	// MaxSeries is the number of series a shard aims to stay under. Zero
	// disables the series criterion.
	MaxSeries uint64
	// HashWithout names series labels left out of the hash that places a
	// series in a shard, so that series differing only in them - replicas
	// carrying their replica label inside the series - share a shard. It
	// must not change once shards exist: a shard re-split under another
	// hash holds series none of its finer shards claims, and the executor
	// refuses such a plan.
	HashWithout []string
}

// Enabled reports whether splitting can happen at all.
func (c SplitConfig) Enabled() bool { return c.MaxShardCount() > 1 }

// MaxShardCount is the largest shard count the configuration allows.
func (c SplitConfig) MaxShardCount() uint64 {
	if c.MaxShards <= 1 {
		return 1
	}
	return prevPow2(uint64(c.MaxShards))
}

// PlannerIndexSizeLimit is the total index size the planner's filter should
// refuse to plan above when splitting is enabled: what the maximum number of
// shards can hold. Below it the executor splits; above it the biggest block
// is marked no-compact as before.
func (c SplitConfig) PlannerIndexSizeLimit(limit int64) int64 {
	if !c.Enabled() || limit <= 0 {
		return limit
	}
	m := c.MaxShardCount()
	if uint64(math.MaxInt64)/m < uint64(limit) {
		return math.MaxInt64
	}
	return limit * int64(m)
}

// PlanEstimate sums what the source blocks report about themselves: index
// bytes from the file stats uploaded with them, and series counts. Both are
// worst-case estimates of the output: deduplication and shared symbols only
// shrink it. Blocks that report no index size contribute zero.
func PlanEstimate(metas []*metadata.Meta) (indexBytes int64, series uint64) {
	for _, m := range metas {
		series += m.Stats.NumSeries
		for _, f := range m.Thanos.Files {
			if f.RelPath == block.IndexFilename {
				indexBytes += f.SizeBytes
			}
		}
	}
	return indexBytes, series
}

// SourceShard returns the shard the plan's sources belong to. All sources of a
// plan share their external labels, so either all carry the shard label or
// none does; count is zero for unsplit sources.
func SourceShard(metas []*metadata.Meta) (index, count uint64, err error) {
	for i, m := range metas {
		idx, cnt, ok, shardErr := metadata.Shard(m.Thanos.Labels)
		if shardErr != nil {
			return 0, 0, errors.Wrapf(shardErr, "block %s", m.ULID)
		}
		if !ok {
			idx, cnt = 0, 0
		}
		if i == 0 {
			index, count = idx, cnt
			continue
		}
		if idx != index || cnt != count {
			return 0, 0, errors.Errorf("plan mixes shards: block %s is %d of %d, an earlier block %d of %d", m.ULID, idx, cnt, index, count)
		}
	}
	return index, count, nil
}

// ShardCount returns how many shards the plan's output is split into, and
// whether the index size limit can be kept at that count.
//
// The estimate measures the sources. When the sources are already shard i of
// sourceShards, it measures that shard's data alone: if it needs k parts,
// each part is one of sourceShards*k shards of the whole stream, and the
// count grows by that factor, not to k. The count is a power of two, never
// below floor - the count the stream already has, so that a later split of
// the stream never creates shard groups that could not merge with the
// existing ones - unless that is above the maximum, never above the maximum,
// and never below the sources' own count, even when a lowered maximum is. A
// count equal to the sources' own count means no split; so does one for
// unsplit sources.
//
// Only the index size is a hard limit, and only it can make a plan not fit:
// when the parts it needs would take the lineage over the maximum, the plan
// has to be refused, as the index size filter refuses one, and the count
// returned is the one it would need. The series target merely sizes the
// split: a plan that would want more shards than allowed for its series is
// split into the maximum and compacted.
func (c SplitConfig) ShardCount(metas []*metadata.Meta, sourceShards, floor uint64) (count uint64, fits bool) {
	if !c.Enabled() {
		return max(sourceShards, 1), true
	}
	indexBytes, series := PlanEstimate(metas)
	byIndex, bySeries := uint64(1), uint64(1)
	if c.MaxIndexSizeBytes > 0 && indexBytes > 0 {
		byIndex = uint64(math.Ceil(float64(indexBytes) / (float64(c.MaxIndexSizeBytes) * indexHeadroom)))
	}
	if c.MaxSeries > 0 && series > 0 {
		bySeries = (series + c.MaxSeries - 1) / c.MaxSeries
	}
	lineage := max(sourceShards, 1)
	// A plan that needs no further split always fits, whatever its lineage's
	// count: lowering the cap below a count the stream already has must not
	// stop ordinary compaction within its shards. A plan that does not fit
	// reports the count it would need.
	need := nextPow2(byIndex) * lineage
	if nextPow2(byIndex) > 1 && need > c.MaxShardCount() {
		return need, false
	}
	count = nextPow2(max(byIndex, bySeries)) * lineage
	count = max(count, floor)
	// The cap bounds how far a plan is split, but never below the count its
	// sources already have: a coarser shard made from one finer shard would
	// carry a label that claims series it does not hold.
	count = max(min(count, c.MaxShardCount()), lineage)
	return count, true
}

// ShardsToProduce lists the 0-based shard indexes of count that a plan whose
// sources are shard sourceIndex of sourceCount produces. Counts are powers of
// two, so a source shard splits into exactly the shards congruent to it modulo
// the source count; unsplit sources produce every shard.
func ShardsToProduce(sourceIndex, sourceCount, count uint64) []uint64 {
	if sourceCount == 0 {
		sourceIndex, sourceCount = 0, 1
	}
	if sourceIndex >= count {
		return nil
	}
	out := make([]uint64, 0, (count-sourceIndex+sourceCount-1)/sourceCount)
	for j := sourceIndex; j < count; j += sourceCount {
		out = append(out, j)
	}
	return out
}

func nextPow2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len64(n-1)
}

func prevPow2(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	return 1 << (bits.Len64(n) - 1)
}

// SplitMetrics counts splits.
type SplitMetrics struct {
	Splits    prometheus.Counter
	Shards    prometheus.Histogram
	Fallbacks prometheus.Counter
}

// NewSplitMetrics registers the split metrics.
func NewSplitMetrics(reg prometheus.Registerer) *SplitMetrics {
	return &SplitMetrics{
		Splits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_block_splits_total",
			Help: "Total number of compactions whose output was split into several blocks by series.",
		}),
		Shards: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "thanos_compact_block_split_shards",
			Help:    "Number of output shards per split compaction.",
			Buckets: []float64{2, 4, 8, 16, 32, 64, 128},
		}),
		Fallbacks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_block_split_fallbacks_total",
			Help: "Total number of compactions that needed more shards than allowed and were not split.",
		}),
	}
}

// splitPlanner adds block splitting to a planner. It decides what a plan
// produces - one block, or one block per shard - and plans the unsplit blocks
// a split stream leaves behind.
//
// Deciding the outputs is planning, not execution: the planner sees the whole
// bucket, so it can floor a stream's shard count and fit a straggler into the
// shards already covering its range, while an executor - in this process or a
// worker elsewhere - only has to produce what the plan says.
//
// Stragglers: once a stream's plans are split, its unsplit group only receives
// fresh blocks, and those compact with each other until their plan is large
// enough to split. But a block can end up alone in its range: the window a
// replica missed while its neighbors were merged vertically and split, a
// late upload, a backfill. The planner never compacts a lone block, so it
// would stay unsplit forever - never downsampled, never retired. When the
// stream's shards already cover such a block's range, no unsplit sibling can
// be expected any more, and the block is planned on its own and split into
// the covering shards.
type splitPlanner struct {
	inner              Planner
	logger             log.Logger
	conf               SplitConfig
	metrics            *SplitMetrics
	bkt                objstore.Bucket
	markedForNoCompact prometheus.Counter
	metas              func() map[ulid.ULID]*metadata.Meta
	noCompact          func() map[ulid.ULID]*metadata.NoCompactMark
	streamShards       func(labels map[string]string, resolution int64, source ShardRef) uint64
	streamLeaves       func(labels map[string]string, resolution, mint, maxt int64, source ShardRef) []ShardRef
}

var (
	_ Planner            = &splitPlanner{}
	_ OutputPlanner      = &splitPlanner{}
	_ SingleBlockPlanner = &splitPlanner{}
)

// WithBlockSplitting wraps a planner so that plans whose output is estimated
// to exceed the configured limits are split into shard blocks by series, and
// so that, when the inner planner has nothing to plan for an unsplit group, an
// unsplit block whose range the stream's shards already cover is planned alone
// for splitting. A plan that would need more shards than the configuration
// allows is refused the way the index size filter refuses one: its biggest
// source block is marked no-compact in bkt, with the same reason, and
// markedForNoCompact counts it. metas is the compactor's synced view of the
// bucket, across groups; metrics may be nil.
func WithBlockSplitting(
	inner Planner,
	logger log.Logger,
	conf SplitConfig,
	metrics *SplitMetrics,
	bkt objstore.Bucket,
	markedForNoCompact prometheus.Counter,
	metas func() map[ulid.ULID]*metadata.Meta,
	noCompact func() map[ulid.ULID]*metadata.NoCompactMark,
) Planner {
	conf.HashWithout = slices.DeleteFunc(slices.Clone(conf.HashWithout), func(name string) bool { return name == "" })
	slices.Sort(conf.HashWithout)
	conf.HashWithout = slices.Compact(conf.HashWithout)
	return &splitPlanner{
		inner:              inner,
		logger:             logger,
		conf:               conf,
		metrics:            metrics,
		bkt:                bkt,
		markedForNoCompact: markedForNoCompact,
		metas:              metas,
		noCompact:          noCompact,
		streamShards:       StreamShardCountFunc(metas),
		streamLeaves:       StreamLeavesFunc(metas),
	}
}

// PlansSingleBlockGroups implements SingleBlockPlanner: a straggler is, by
// definition, alone in its group.
func (p *splitPlanner) PlansSingleBlockGroups() bool { return true }

// decide returns the shards a plan for the group produces, or none when the
// plan is not split, and whether the configuration allows them.
//
// Unsplit blocks in a range the stream's shards already cover are split into
// exactly those shards; otherwise the estimate decides, never below the count
// the stream has.
func (p *splitPlanner) decide(
	ctx context.Context,
	groupLabels map[string]string,
	resolution int64,
	sources []*metadata.Meta,
) (pieces []ShardRef, count uint64, fits bool, err error) {
	if err := p.lookupIndexSizes(ctx, sources); err != nil {
		return nil, 0, false, err
	}
	sourceShard, sourceShards, err := SourceShard(sources)
	if err != nil {
		return nil, 0, false, errors.Wrap(err, "shard of the plan's sources")
	}
	planMinT, planMaxT := sources[0].MinTime, sources[0].MaxTime
	for _, m := range sources[1:] {
		planMinT, planMaxT = min(planMinT, m.MinTime), max(planMaxT, m.MaxTime)
	}
	source := ShardRef{Index: sourceShard, Count: sourceShards}
	pieces = p.streamLeaves(streamLabels(groupLabels), resolution, planMinT, planMaxT, source)
	if len(pieces) > 0 {
		return pieces, 0, true, nil
	}
	// A lineage never goes back below its finest count: once one range of a
	// shard group has been re-split, its other ranges have to follow, or the
	// two counts could never compact into one range again. For unsplit
	// sources the lineage is the whole stream.
	floor := p.streamShards(streamLabels(groupLabels), resolution, source)
	count, fits = p.conf.ShardCount(sources, sourceShards, floor)
	if !fits || count <= max(sourceShards, 1) {
		return nil, count, fits, nil
	}
	shards := ShardsToProduce(sourceShard, sourceShards, count)
	pieces = make([]ShardRef, len(shards))
	for i, j := range shards {
		pieces[i] = ShardRef{Index: j, Count: count}
	}
	return pieces, count, true, nil
}

// lookupIndexSizes fills in the index size of every source whose metadata
// does not record it, as the index size filter does, so that the estimate
// never counts such a block as empty. The size is kept in the metadata for
// the rest of the sync.
func (p *splitPlanner) lookupIndexSizes(ctx context.Context, sources []*metadata.Meta) error {
	if p.bkt == nil {
		return nil
	}
	for _, m := range sources {
		known := false
		for _, f := range m.Thanos.Files {
			if f.RelPath == block.IndexFilename && f.SizeBytes > 0 {
				known = true
				break
			}
		}
		if known {
			continue
		}
		attr, err := p.bkt.Attributes(ctx, filepath.Join(m.ULID.String(), block.IndexFilename))
		if p.bkt.IsObjNotFoundErr(err) {
			// Nothing to measure; whatever is wrong with the block, the
			// compaction itself will say.
			continue
		}
		if err != nil {
			return errors.Wrapf(err, "index size of block %s", m.ULID)
		}
		m.Thanos.Files = slices.DeleteFunc(slices.Clone(m.Thanos.Files), func(f metadata.File) bool { return f.RelPath == block.IndexFilename })
		m.Thanos.Files = append(m.Thanos.Files, metadata.File{RelPath: block.IndexFilename, SizeBytes: attr.Size})
	}
	return nil
}

// PlanOutputs implements OutputPlanner. The sources of a plan share their
// shard, if any; a split produces the shards congruent to it. Nothing is
// returned when the plan is not split, in which case the output keeps the
// group's labels - and with them the sources' shard.
func (p *splitPlanner) PlanOutputs(ctx context.Context, cg *Group, sources []*metadata.Meta) ([]PlanOutput, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	groupLabels := cg.Labels().Map()
	pieces, count, fits, err := p.decide(ctx, groupLabels, cg.Resolution(), sources)
	if err != nil {
		return nil, err
	}
	if !fits {
		// Plan refuses such a plan before it gets here.
		return nil, errors.Errorf("the plan would need %d shards, more than the %d allowed", count, p.conf.MaxShardCount())
	}
	if len(pieces) == 0 {
		return nil, nil
	}
	_, sourceShards, _ := SourceShard(sources)

	outputs := make([]PlanOutput, 0, len(pieces))
	labelsOf := make([]string, 0, len(pieces))
	for _, piece := range pieces {
		lbls := maps.Clone(groupLabels)
		lbls[metadata.CompactorShardLabel] = piece.Label()
		outputs = append(outputs, PlanOutput{Labels: lbls, Series: &SeriesPartition{Index: piece.Index, Count: piece.Count, Without: p.conf.HashWithout}})
		labelsOf = append(labelsOf, piece.Label())
	}
	level.Info(p.logger).Log("msg", "splitting compaction by series", "group", cg.Key(), "shards", fmt.Sprintf("%v", labelsOf), "sourceShards", sourceShards, "sources", len(sources))
	if p.metrics != nil {
		p.metrics.Splits.Inc()
		p.metrics.Shards.Observe(float64(len(pieces)))
	}
	return outputs, nil
}

// PlanSiblings implements SiblingPlanner: a plan names every block in the
// compactor's view that shares a set with one of its sources, so that those
// stay published once the sources are gone. See SetSiblings.
func (p *splitPlanner) PlanSiblings(_ context.Context, _ *Group, sources []*metadata.Meta) ([]ulid.ULID, error) {
	return SetSiblings(p.metas(), sources), nil
}

// Plan implements Planner. A plan the inner planner makes is refused when its
// lineage cannot produce enough shards under the cap: as with the index size
// filter, the biggest source block is marked no-compact and planning goes on
// without it. The index size filter alone cannot judge this, since what a
// shard group can still be split into depends on the count it already has.
func (p *splitPlanner) Plan(ctx context.Context, metasByMinTime []*metadata.Meta, errChan chan error, extensions any) ([]*metadata.Meta, error) {
	candidates := metasByMinTime
	for range len(metasByMinTime) + 1 {
		plan, err := p.inner.Plan(ctx, candidates, errChan, extensions)
		if err != nil {
			return nil, err
		}
		if len(plan) == 0 {
			break
		}
		// The group's labels are its blocks' labels, replica labels removed.
		_, count, fits, err := p.decide(ctx, plan[0].Thanos.Labels, plan[0].Thanos.Downsample.Resolution, plan)
		if err != nil {
			return nil, err
		}
		if fits {
			return plan, nil
		}
		biggest := p.refuse(ctx, plan, count)
		if biggest == nil {
			return nil, nil
		}
		candidates = slices.DeleteFunc(slices.Clone(candidates), func(m *metadata.Meta) bool { return m.ULID == biggest.ULID })
	}
	if len(metasByMinTime) == 0 {
		return nil, nil
	}
	source, err := groupShard(metasByMinTime[0])
	if err != nil {
		return nil, err
	}
	covered := shardCoverage(metasByMinTime[0], source, p.metas())
	// A shard block whose lineage has moved on to a finer count anywhere in
	// the stream has no future at its own: no range at that count can ever
	// be completed again. It is split up to the lineage's count, covered or
	// not - as far as the cap allows; a count the cap no longer allows is not
	// one the block can be split towards. An unsplit block is different - it
	// may be a fresh upload waiting for its siblings - so for it coverage
	// remains the only guard.
	lineage := p.streamShards(streamLabels(metasByMinTime[0].Thanos.Labels), metasByMinTime[0].Thanos.Downsample.Resolution, source)
	behind := source.Count > 0 && min(lineage, p.conf.MaxShardCount()) > source.Count
	if len(covered) == 0 && !behind {
		return nil, nil
	}
	var noCompact map[ulid.ULID]*metadata.NoCompactMark
	if p.noCompact != nil {
		noCompact = p.noCompact()
	}
	// Coverage is the only guard for unsplit blocks: a block whose range the
	// stream's shards already cover is not a fresh upload waiting for
	// siblings, whatever its position in the unsplit group - after the other
	// stragglers are gone it is the group's newest block, and it must still
	// be split. The candidates exclude blocks refused above, whose marks the
	// snapshot of no-compact marks does not hold yet.
	var (
		pieces []ShardRef
		count  uint64
		fits   bool
	)
	for _, m := range candidates {
		if _, excluded := noCompact[m.ULID]; excluded {
			continue
		}
		if !behind && !coversRange(covered, m.MinTime, m.MaxTime) {
			continue
		}
		// A block planned alone is refused like any plan when its split
		// would need more shards than allowed, and skipped when it would
		// not be split at all: it would only be rewritten as itself, pass
		// after pass.
		pieces, count, fits, err = p.decide(ctx, m.Thanos.Labels, m.Thanos.Downsample.Resolution, []*metadata.Meta{m})
		if err != nil {
			return nil, err
		}
		if !fits {
			p.refuse(ctx, []*metadata.Meta{m}, count)
			continue
		}
		if len(pieces) == 0 {
			continue
		}
		level.Info(p.logger).Log("msg", "splitting a block left behind in a split stream", "block", m.ULID, "shard", source.Label(), "mint", m.MinTime, "maxt", m.MaxTime)
		return []*metadata.Meta{m}, nil
	}
	return nil, nil
}

// refuse marks the biggest source block of a plan that would need more shards
// than allowed no-compact, so that it is not planned again, and returns it.
// The mark carries the index size filter's reason, so existing alerts fire.
func (p *splitPlanner) refuse(ctx context.Context, plan []*metadata.Meta, count uint64) *metadata.Meta {
	var biggest *metadata.Meta
	var biggestBytes int64 = -1
	for _, m := range plan {
		bytes, _ := PlanEstimate([]*metadata.Meta{m})
		if bytes > biggestBytes || (bytes == biggestBytes && m.Stats.NumSeries > biggest.Stats.NumSeries) {
			biggest, biggestBytes = m, bytes
		}
	}
	if p.metrics != nil {
		p.metrics.Fallbacks.Inc()
	}
	level.Warn(p.logger).Log("msg", "a compaction would need more shards than allowed; marking its biggest block for no compaction",
		"block", biggest.ULID, "shardsNeeded", count, "maxShards", p.conf.MaxShardCount())
	if p.bkt == nil {
		return biggest
	}
	if err := block.MarkForNoCompact(
		ctx,
		p.logger,
		p.bkt,
		biggest.ULID,
		metadata.IndexSizeExceedingNoCompactReason,
		fmt.Sprintf("splitPlanner: the compaction would need %d shards, over --compact.block-split.max-shards=%d; raise the cap or lower the per-shard limits. See https://github.com/thanos-io/thanos/issues/1424", count, p.conf.MaxShardCount()),
		p.markedForNoCompact,
	); err != nil {
		level.Warn(p.logger).Log("msg", "could not mark the block for no compaction; it is skipped this pass", "block", biggest.ULID, "err", err)
	}
	return biggest
}

type timeRange struct{ mint, maxt int64 }

// groupShard returns the shard of a group's blocks; a zero count means an
// unsplit group.
func groupShard(m *metadata.Meta) (ShardRef, error) {
	index, count, ok, err := metadata.Shard(m.Thanos.Labels)
	if err != nil {
		return ShardRef{}, errors.Wrapf(err, "block %s", m.ULID)
	}
	if !ok {
		return ShardRef{}, nil
	}
	return ShardRef{Index: index, Count: count}, nil
}

// refines reports whether shard r holds a part of the series of source: the
// same shard, or a finer one of its lineage. An unsplit source is refined by
// every shard.
func (r ShardRef) refines(source ShardRef) bool {
	if source.Count == 0 {
		return true
	}
	return r.Count > source.Count && r.Count%source.Count == 0 && r.Index%source.Count == source.Index
}

// shardCoverage returns the merged time ranges of the shard blocks of the
// same stream and resolution as the given block that refine its shard: for an
// unsplit block every shard, for a shard block the finer shards of its lineage.
func shardCoverage(m *metadata.Meta, source ShardRef, all map[ulid.ULID]*metadata.Meta) []timeRange {
	stream := streamLabels(m.Thanos.Labels)
	var ranges []timeRange
	for _, o := range all {
		if o.Thanos.Downsample.Resolution != m.Thanos.Downsample.Resolution || !sameStream(o.Thanos.Labels, stream) {
			continue
		}
		index, count, ok, err := metadata.Shard(o.Thanos.Labels)
		if err != nil || !ok || !(ShardRef{index, count}).refines(source) {
			continue
		}
		ranges = append(ranges, timeRange{o.MinTime, o.MaxTime})
	}
	if len(ranges) == 0 {
		return nil
	}
	slices.SortFunc(ranges, func(a, b timeRange) int { return cmp.Compare(a.mint, b.mint) })
	merged := ranges[:1]
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if r.mint <= last.maxt {
			last.maxt = max(last.maxt, r.maxt)
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

// sameStream reports whether the shard's labels without the shard label are
// exactly the unsplit block's labels.
func sameStream(shard, unsplit map[string]string) bool {
	if len(shard) != len(unsplit)+1 {
		return false
	}
	for k, v := range unsplit {
		if shard[k] != v {
			return false
		}
	}
	return true
}

func coversRange(ranges []timeRange, mint, maxt int64) bool {
	for _, r := range ranges {
		if r.mint <= mint && maxt <= r.maxt {
			return true
		}
	}
	return false
}

// StreamShardCountFunc returns a function reporting the largest shard count
// present in a lineage of a stream - the unsplit labels and the resolution
// given - in the compactor's synced view: among the shards that refine the
// given source shard, which for an unsplit source is every shard of the
// stream. Zero if there are none.
func StreamShardCountFunc(
	metas func() map[ulid.ULID]*metadata.Meta,
) func(labels map[string]string, resolution int64, source ShardRef) uint64 {
	return func(unsplit map[string]string, resolution int64, source ShardRef) uint64 {
		var most uint64
		for _, m := range metas() {
			if m.Thanos.Downsample.Resolution != resolution || !sameStream(m.Thanos.Labels, unsplit) {
				continue
			}
			if index, count, ok, err := metadata.Shard(m.Thanos.Labels); err == nil && ok && (ShardRef{index, count}).refines(source) {
				most = max(most, count)
			}
		}
		return most
	}
}

// StreamHasNewerBlock reports whether the block stream of the group - the
// same labels apart from the compactor's shard label, at the same resolution -
// has a block in another of its groups that starts after the group's newest.
func StreamHasNewerBlock(metasByMinTime []*metadata.Meta, all map[ulid.ULID]*metadata.Meta) bool {
	if len(metasByMinTime) == 0 {
		return false
	}
	newest := metasByMinTime[len(metasByMinTime)-1]
	base := streamLabels(newest.Thanos.Labels)
	for _, m := range all {
		if m.MinTime <= newest.MinTime || m.Thanos.Downsample.Resolution != newest.Thanos.Downsample.Resolution {
			continue
		}
		if maps.Equal(streamLabels(m.Thanos.Labels), base) {
			return true
		}
	}
	return false
}

// streamLabels are the block's labels without the compactor's shard label.
func streamLabels(lbls map[string]string) map[string]string {
	if _, ok := lbls[metadata.CompactorShardLabel]; !ok {
		return lbls
	}
	out := make(map[string]string, len(lbls)-1)
	for k, v := range lbls {
		if k != metadata.CompactorShardLabel {
			out[k] = v
		}
	}
	return out
}

// ShardRef names one shard: index of count, 0-based.
type ShardRef struct {
	Index uint64
	Count uint64
}

// Label is the shard's label value.
func (r ShardRef) Label() string { return metadata.FormatShardLabelValue(r.Index, r.Count) }

// StreamLeavesFunc returns a function listing the shards a plan has to be
// split into when shards of its stream already cover the plan's time range and
// refine the plan's own shard: exactly the shards present over that range,
// whatever their counts, so that the pieces join the groups that hold the rest
// of the range's series and compact with them. Parts of the plan's hash space
// no present shard covers - a shard that was empty when the range was split -
// get new shards at the finest count present. It returns nothing when no such
// shard covers the whole range, in which case the plan is a fresh split.
//
// The labels are the stream's, without the shard label; source is the plan's
// own shard, with a zero count for unsplit blocks.
func StreamLeavesFunc(
	metas func() map[ulid.ULID]*metadata.Meta,
) func(labels map[string]string, resolution, mint, maxt int64, source ShardRef) []ShardRef {
	return func(stream map[string]string, resolution, mint, maxt int64, source ShardRef) []ShardRef {
		seen := map[ShardRef]struct{}{}
		var finest uint64
		for _, m := range metas() {
			if m.Thanos.Downsample.Resolution != resolution || m.MinTime > mint || m.MaxTime < maxt || !sameStream(m.Thanos.Labels, stream) {
				continue
			}
			index, count, ok, err := metadata.Shard(m.Thanos.Labels)
			if err != nil || !ok {
				continue
			}
			r := ShardRef{index, count}
			if !r.refines(source) {
				continue
			}
			seen[r] = struct{}{}
			finest = max(finest, count)
		}
		if len(seen) == 0 {
			return nil
		}
		leaves := slices.Collect(maps.Keys(seen))
		for j := range finest {
			if !(ShardRef{j, finest}).refines(source) {
				continue // Not this plan's series.
			}
			covered := false
			for r := range seen {
				if j%r.Count == r.Index {
					covered = true
					break
				}
			}
			if !covered {
				leaves = append(leaves, ShardRef{j, finest})
			}
		}
		slices.SortFunc(leaves, func(a, b ShardRef) int {
			if a.Count != b.Count {
				return cmp.Compare(a.Count, b.Count)
			}
			return cmp.Compare(a.Index, b.Index)
		})
		return leaves
	}
}

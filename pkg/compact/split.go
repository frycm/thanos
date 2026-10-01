// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// SplitGrouper splits large streams into a fixed number of shards at the first compaction level, so that no block of
// such a stream holds all of its series. It wraps the compaction groups of a DefaultGrouper (or of a
// ConcurrentJobsGrouper over it) and adds split jobs:
//
//   - A stream (blocks with the same external labels, ignoring metadata.CompactorShardIDLabel) is split into M shards,
//     M a power of two: SplitConfig.ShardsFor, unless the stream already has shard blocks in the largest compaction
//     range (14d by default) the split falls into; then the scheme those blocks record (count and ignored labels) is
//     used, so that a configuration change applies from the next largest range and the shards of one range never
//     mix counts or hashes. A window whose range holds shard blocks recording no scheme, an unknown hash or different
//     schemes is never split: the compactor halts when the stream is configured to be split.
//   - A window of the smallest range (2h) of raw blocks is split when all unsplit blocks overlapping it lie within it
//     (fresh blocks from Prometheus or Receive, replicas included), none of them is marked for no compaction, and it
//     is not the newest window of the stream (its replicas may still be uploading). Unsplit blocks longer than the
//     window (history from before splitting was enabled) are never split; the windows they overlap compact the
//     legacy way.
//   - A split job compacts all unsplit blocks of a window into the series of one shard (PartitionedBlockPopulator):
//     replicas are deduplicated inside the shard by the vertical compaction merge function. Its output carries the
//     stream's labels plus metadata.CompactorShardIDLabel="<i>_of_<M>" and records the metadata.SplitScheme in its
//     extensions. A shard whose block with sources containing the window's blocks already exists is not split again.
//   - Split jobs never mark their sources for deletion. An unsplit block is retired by block.DefaultDeduplicateFilter
//     (and garbage collection) only once every shard of a split of it exists, so a split that failed part-way never
//     loses data: the unsplit blocks stay and the missing shards are split again in the next pass.
//   - The unsplit blocks of a window being split are kept out of the normal planning of their stream (they are
//     listed by NoCompactMarkedBlocks, which the planners must be given), so that they are never merged into a block
//     that no shard family could retire.
//   - Shard blocks compact further as ordinary groups, one per shard label value. When a stream moves to another
//     shard count, the shards of the old count stop receiving blocks; their compaction groups get two virtual blocks
//     after their newest one, so that the planner does not keep their newest blocks out of compaction forever.
//
// Without shard blocks in the bucket and without any stream configured to be split, SplitGrouper returns the groups of
// the wrapped grouper unchanged.
type SplitGrouper struct {
	logger    log.Logger
	base      *DefaultGrouper
	inner     Grouper
	ranges    []int64
	cfg       SplitConfig
	noCompact NoCompactMarks

	mtx sync.RWMutex
	// excluded are the blocks kept out of normal planning in the current pass.
	excluded map[ulid.ULID]*metadata.NoCompactMark

	plannedSplitJobs prometheus.Gauge
	excludedBlocks   prometheus.Gauge
	closedLanes      prometheus.Gauge
}

var (
	_ Grouper        = &SplitGrouper{}
	_ NoCompactMarks = &SplitGrouper{}
)

// NewSplitGrouper returns a SplitGrouper over the groups of base, split into concurrent jobs (see
// ConcurrentJobsGrouper) when concurrentJobs is set. The ranges must be the ones the planner and compactor use.
// noCompact lists the blocks marked for no compaction in the bucket. The planners of the BucketCompactor must be
// given the SplitGrouper as their NoCompactMarks and wrapped with Planner; the BucketCompactor must use the
// SplitGrouper's Compactor, BlockDeletableChecker and CompactionLifecycleCallback wrappers.
func NewSplitGrouper(
	logger log.Logger,
	base *DefaultGrouper,
	concurrentJobs bool,
	ranges []int64,
	cfg SplitConfig,
	noCompact NoCompactMarks,
	reg prometheus.Registerer,
) (*SplitGrouper, error) {
	if len(ranges) == 0 {
		return nil, errors.New("no compaction ranges")
	}
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid split configuration")
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	g := &SplitGrouper{
		logger:    logger,
		base:      base,
		ranges:    ranges,
		cfg:       cfg,
		noCompact: noCompact,
		plannedSplitJobs: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_split_planned_jobs",
			Help: "Number of split jobs (one per shard of a window of a stream) planned in the last compaction pass.",
		}),
		excludedBlocks: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_split_excluded_blocks",
			Help: "Number of unsplit blocks kept out of normal planning in the last compaction pass because their window is being split.",
		}),
		closedLanes: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_split_closed_shard_groups",
			Help: "Number of shard compaction groups that no longer receive blocks because their stream moved to another shard count.",
		}),
	}
	g.inner = base
	if concurrentJobs {
		g.inner = NewConcurrentJobsGrouper(base, ranges, g)
	}
	return g, nil
}

// NoCompactMarkedBlocks returns the blocks marked for no compaction in the bucket, plus the blocks kept out of normal
// planning in the current pass: the unsplit blocks of windows being split and the virtual blocks closing shard groups.
func (g *SplitGrouper) NoCompactMarkedBlocks() map[ulid.ULID]*metadata.NoCompactMark {
	marks := g.noCompact.NoCompactMarkedBlocks()
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	if len(g.excluded) == 0 {
		return marks
	}
	res := make(map[ulid.ULID]*metadata.NoCompactMark, len(marks)+len(g.excluded))
	maps.Copy(res, g.excluded)
	maps.Copy(res, marks)
	return res
}

// Groups returns the groups of the wrapped grouper, which never plan the unsplit blocks of windows being split, and
// one group per split job.
func (g *SplitGrouper) Groups(blocks map[ulid.ULID]*metadata.Meta) ([]*Group, error) {
	plan, err := g.plan(blocks, g.noCompact.NoCompactMarkedBlocks())
	if err != nil {
		return nil, err
	}

	g.mtx.Lock()
	g.excluded = plan.excluded
	g.mtx.Unlock()
	g.plannedSplitJobs.Set(float64(len(plan.jobs)))
	g.excludedBlocks.Set(float64(len(plan.excluded) - len(plan.laneEnds)))
	g.closedLanes.Set(float64(len(plan.laneEnds) / 2))

	view := blocks
	if len(plan.laneEnds) > 0 {
		view = maps.Clone(blocks)
		for _, m := range plan.laneEnds {
			view[m.ULID] = m
		}
	}
	groups, err := g.inner.Groups(view)
	if err != nil {
		return nil, err
	}

	for _, job := range plan.jobs {
		jg, err := g.splitJobGroup(job)
		if err != nil {
			return nil, errors.Wrapf(err, "create split job group for %s", job)
		}
		groups = append(groups, jg)
	}
	if len(plan.jobs) > 0 {
		level.Info(g.logger).Log("msg", "planned split jobs", "jobs", len(plan.jobs), "excluded_blocks", len(plan.excluded)-len(plan.laneEnds))
	}
	return groups, nil
}

// splitJobSpec is one shard of the split of a window of a stream.
type splitJobSpec struct {
	streamLabels     labels.Labels
	minTime, maxTime int64
	index            int
	scheme           metadata.SplitScheme
	// metas are the unsplit blocks of the window, sorted by MinTime.
	metas []*metadata.Meta
}

func (j splitJobSpec) shardID() string {
	return metadata.FormatShardID(j.index, j.scheme.Shards)
}

func (j splitJobSpec) String() string {
	ids := make([]string, 0, len(j.metas))
	for _, m := range j.metas {
		ids = append(ids, m.ULID.String())
	}
	return fmt.Sprintf("shard %s of %s [%d, %d): %v", j.shardID(), j.streamLabels, j.minTime, j.maxTime, ids)
}

// splitPlan is what SplitGrouper does in one pass.
type splitPlan struct {
	jobs []splitJobSpec
	// excluded are the blocks the planners must not compact: the unsplit blocks of the windows being split and the
	// lane end markers.
	excluded map[ulid.ULID]*metadata.NoCompactMark
	// laneEnds are the virtual blocks added to the compaction groups of shards that no longer receive blocks.
	laneEnds []*metadata.Meta
}

// splitStream holds the raw unsplit blocks and the shard blocks of one stream.
type splitStream struct {
	lset    labels.Labels
	unsplit []*metadata.Meta
	shards  []shardMeta
	// newestRaw is the largest MinTime of the stream's raw blocks, unsplit or shards.
	newestRaw int64
	hasRaw    bool
}

type shardMeta struct {
	*metadata.Meta
	shardID      string
	index, count int
}

// rangeScheme is the split scheme the shard blocks of one largest compaction range of a stream agree on.
type rangeScheme struct {
	scheme metadata.SplitScheme
	// err tells why the shard blocks of the range cannot be split further.
	err error
}

func (g *SplitGrouper) plan(blocks map[ulid.ULID]*metadata.Meta, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark) (splitPlan, error) {
	plan := splitPlan{excluded: map[ulid.ULID]*metadata.NoCompactMark{}}
	if !g.cfg.Enabled() && !hasShardBlocks(blocks) {
		return plan, nil
	}

	streams := map[string]*splitStream{}
	for _, m := range blocks {
		streamLabels := m.Thanos.StreamLabels()
		if _, ok := streamLabels[metadata.CompactorShardIDLabel]; ok {
			// A shard label that does not parse: the block is not part of a split this compactor knows of.
			continue
		}
		shardID, isShard := m.Thanos.ShardID()
		if !isShard && m.Thanos.Downsample.Resolution != downsample.ResLevel0 {
			continue
		}
		lset := labels.FromMap(streamLabels)
		key := lset.String()
		st, ok := streams[key]
		if !ok {
			st = &splitStream{lset: lset}
			streams[key] = st
		}
		if isShard {
			index, count, _ := metadata.ParseShardID(shardID)
			st.shards = append(st.shards, shardMeta{Meta: m, shardID: shardID, index: index, count: count})
		} else {
			st.unsplit = append(st.unsplit, m)
		}
		if m.Thanos.Downsample.Resolution == downsample.ResLevel0 {
			if !st.hasRaw || m.MinTime > st.newestRaw {
				st.newestRaw = m.MinTime
			}
			st.hasRaw = true
		}
	}

	keys := make([]string, 0, len(streams))
	for k, st := range streams {
		if len(st.shards) == 0 && g.cfg.ShardsFor(st.lset) <= 1 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := g.planStream(streams[k], noCompactMarked, &plan); err != nil {
			return splitPlan{}, err
		}
	}
	return plan, nil
}

func hasShardBlocks(blocks map[ulid.ULID]*metadata.Meta) bool {
	for _, m := range blocks {
		if _, ok := m.Thanos.ShardID(); ok {
			return true
		}
	}
	return false
}

func (g *SplitGrouper) planStream(st *splitStream, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark, plan *splitPlan) error {
	var (
		largestRange = g.ranges[len(g.ranges)-1]
		configured   = g.cfg.ShardsFor(st.lset)
	)

	// The scheme of each largest range holding shard blocks of the stream, at any resolution.
	schemes := map[int64]*rangeScheme{}
	for _, s := range st.shards {
		for r := alignedRangeStart(s.MinTime, largestRange); r < max(s.MaxTime, s.MinTime+1); r += largestRange {
			rs, ok := schemes[r]
			if !ok {
				rs = &rangeScheme{}
				schemes[r] = rs
			}
			if rs.err != nil {
				continue
			}
			scheme, err := shardScheme(s)
			if err != nil {
				rs.err = err
				continue
			}
			if !ok {
				rs.scheme = scheme
				continue
			}
			if !rs.scheme.Equal(scheme) {
				rs.err = errors.Errorf("shard block %s (%s) records split scheme %s but other shard blocks of the same range record %s", s.ULID, s.shardID, scheme, rs.scheme)
			}
		}
	}

	if st.hasRaw && len(st.unsplit) > 0 {
		if err := g.planWindows(st, schemes, configured, noCompactMarked, plan); err != nil {
			return err
		}
	}

	if st.hasRaw && len(st.shards) > 0 {
		g.closeLanes(st, schemes, configured, plan)
	}
	return nil
}

// shardScheme returns the split scheme a shard block records, checked against its shard label.
func shardScheme(s shardMeta) (metadata.SplitScheme, error) {
	scheme, err := s.Thanos.SplitScheme()
	if err != nil {
		return metadata.SplitScheme{}, errors.Wrapf(err, "shard block %s (%s)", s.ULID, s.shardID)
	}
	if scheme == nil {
		return metadata.SplitScheme{}, errors.Errorf("shard block %s (%s) records no split scheme in its %q extension; it was not written by this compactor", s.ULID, s.shardID, metadata.CompactorSplitExtensionKey)
	}
	if scheme.Hash != metadata.SplitHashStable {
		return metadata.SplitScheme{}, errors.Errorf("shard block %s (%s) records split hash %q, this compactor only knows %q", s.ULID, s.shardID, scheme.Hash, metadata.SplitHashStable)
	}
	if scheme.Shards != s.count {
		return metadata.SplitScheme{}, errors.Errorf("shard block %s (%s) records a split scheme with %d shards", s.ULID, s.shardID, scheme.Shards)
	}
	return *scheme, nil
}

// splitWindow holds the unsplit raw blocks of a window of the smallest compaction range.
type splitWindow struct {
	start   int64
	metas   []*metadata.Meta
	blocked bool
}

func (g *SplitGrouper) planWindows(st *splitStream, schemes map[int64]*rangeScheme, configured int, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark, plan *splitPlan) error {
	var (
		windowRange  = g.ranges[0]
		largestRange = g.ranges[len(g.ranges)-1]
		newestWindow = alignedRangeStart(st.newestRaw, windowRange)
		windows      = map[int64]*splitWindow{}
		// legacy are the unsplit blocks that do not fit in a window: they are never split, and nor are the windows
		// they overlap.
		legacy []*metadata.Meta
	)
	for _, m := range st.unsplit {
		start := alignedRangeStart(m.MinTime, windowRange)
		if m.MaxTime > start+windowRange {
			legacy = append(legacy, m)
			continue
		}
		w, ok := windows[start]
		if !ok {
			w = &splitWindow{start: start}
			windows[start] = w
		}
		w.metas = append(w.metas, m)
		if _, marked := noCompactMarked[m.ULID]; marked || m.Compaction.Failed {
			w.blocked = true
		}
	}

	rawShards := indexRawShards(st.shards)
	starts := slices.Sorted(maps.Keys(windows))
	for _, start := range starts {
		w := windows[start]
		end := start + windowRange
		if w.blocked || start == newestWindow {
			continue
		}
		if slices.ContainsFunc(legacy, func(m *metadata.Meta) bool { return m.MinTime < end && start < m.MaxTime }) {
			continue
		}

		var scheme metadata.SplitScheme
		if rs, ok := schemes[alignedRangeStart(start, largestRange)]; ok {
			if rs.err != nil {
				if configured <= 1 {
					// Not configured to split this stream: leave the window to normal compaction rather than halt.
					level.Warn(g.logger).Log("msg", "not splitting a window whose compaction range has shard blocks of an unknown or inconsistent split scheme", "stream", st.lset, "window_start", start, "err", rs.err)
					continue
				}
				return halt(errors.Wrapf(rs.err, "refusing to split [%d, %d) of stream %s: the shard blocks of its compaction range do not record one split scheme this compactor can continue", start, end, st.lset))
			}
			scheme = rs.scheme
			if configured > 1 && !scheme.Equal(g.cfg.scheme(configured)) {
				level.Debug(g.logger).Log("msg", "splitting with the scheme of the existing shard blocks of the compaction range; the configured one applies from the next range",
					"stream", st.lset, "window_start", start, "scheme", scheme, "configured", g.cfg.scheme(configured))
			}
		} else {
			if configured <= 1 {
				continue
			}
			scheme = g.cfg.scheme(configured)
		}
		if scheme.Shards <= 1 {
			continue
		}

		metas := sortedByMinTime(w.metas)
		for _, m := range metas {
			plan.excluded[m.ULID] = &metadata.NoCompactMark{ID: m.ULID, Version: metadata.NoCompactMarkVersion1, Details: "unsplit block of a window being split"}
		}
		sources := uniqueSortedSources(metas)
		for i := 1; i <= scheme.Shards; i++ {
			if rawShards.covers(i, scheme.Shards, start, end, largestRange, sources) {
				continue
			}
			plan.jobs = append(plan.jobs, splitJobSpec{
				streamLabels: st.lset,
				minTime:      start,
				maxTime:      end,
				index:        i,
				scheme:       scheme,
				metas:        metas,
			})
		}
	}
	return nil
}

// rawShardIndex holds the raw shard blocks of a stream by shard, sorted by MinTime, with their sorted sources.
type rawShardIndex map[[2]int][]rawShard

type rawShard struct {
	*metadata.Meta
	sources []ulid.ULID
}

func indexRawShards(shards []shardMeta) rawShardIndex {
	idx := rawShardIndex{}
	for _, s := range shards {
		if s.Thanos.Downsample.Resolution != downsample.ResLevel0 {
			continue
		}
		k := [2]int{s.index, s.count}
		idx[k] = append(idx[k], rawShard{Meta: s.Meta, sources: uniqueSortedSources([]*metadata.Meta{s.Meta})})
	}
	for _, lane := range idx {
		sort.Slice(lane, func(i, j int) bool { return lane[i].MinTime < lane[j].MinTime })
	}
	return idx
}

// covers tells whether a raw block of shard index of count overlaps [mint, maxt) and has sources that contain the
// given sorted sources. Shard blocks never span more than the largest compaction range, so only the blocks starting
// at most that long before mint are looked at.
func (idx rawShardIndex) covers(index, count int, mint, maxt, largestRange int64, sources []ulid.ULID) bool {
	lane := idx[[2]int{index, count}]
	for i := sort.Search(len(lane), func(i int) bool { return lane[i].MinTime >= mint-largestRange }); i < len(lane) && lane[i].MinTime < maxt; i++ {
		if lane[i].MaxTime > mint && containsAllSorted(lane[i].sources, sources) {
			return true
		}
	}
	return false
}

// closeLanes adds two virtual blocks after the newest block of each shard compaction group (lane) that no longer
// receives blocks: its stream has raw blocks after the end of the largest compaction range of the lane's newest
// block, and the newest range of the stream uses another shard count. Like any planner, the default and the
// concurrent jobs planners never compact the newest block of a group, and compact a range that is not complete only
// once it ends before the second newest block starts. Without the virtual blocks, the last blocks of a lane would never
// be compacted to the largest range, and thus never downsampled. The virtual blocks are listed in NoCompactMarkedBlocks,
// lie in the next largest range and overlap nothing, so no plan ever holds them.
func (g *SplitGrouper) closeLanes(st *splitStream, schemes map[int64]*rangeScheme, configured int, plan *splitPlan) {
	largestRange := g.ranges[len(g.ranges)-1]

	newestCount := configured
	if rs, ok := schemes[alignedRangeStart(st.newestRaw, largestRange)]; ok {
		if rs.err != nil {
			// The newest range cannot be split further anyway; leave its lanes alone.
			return
		}
		newestCount = rs.scheme.Shards
	}

	type lane struct {
		labels     map[string]string
		resolution int64
		count      int
		newest     int64
	}
	lanes := map[string]*lane{}
	for _, s := range st.shards {
		key := fmt.Sprintf("%s/%d", s.shardID, s.Thanos.Downsample.Resolution)
		l, ok := lanes[key]
		if !ok {
			lanes[key] = &lane{labels: s.Thanos.Labels, resolution: s.Thanos.Downsample.Resolution, count: s.count, newest: s.MinTime}
			continue
		}
		l.newest = max(l.newest, s.MinTime)
	}
	for _, key := range slices.Sorted(maps.Keys(lanes)) {
		l := lanes[key]
		end := alignedRangeStart(l.newest, largestRange) + largestRange
		if st.newestRaw < end || l.count == newestCount {
			continue
		}
		seed := xxhash.Sum64String(st.lset.String() + "/" + key)
		for i := range 2 {
			var id ulid.ULID
			// Timestamp 0, so it cannot collide with a block written by anything.
			binary.BigEndian.PutUint64(id[8:], seed)
			id[7] = byte(i + 1)
			m := &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					Version:    metadata.TSDBVersion1,
					ULID:       id,
					MinTime:    end + int64(i),
					MaxTime:    end + int64(i) + 1,
					Compaction: tsdb.BlockMetaCompaction{Level: 1, Sources: []ulid.ULID{id}},
				},
				Thanos: metadata.Thanos{
					Version:    metadata.ThanosVersion1,
					Labels:     l.labels,
					Downsample: metadata.ThanosDownsample{Resolution: l.resolution},
					Source:     metadata.CompactorSource,
				},
			}
			plan.laneEnds = append(plan.laneEnds, m)
			plan.excluded[id] = &metadata.NoCompactMark{ID: id, Version: metadata.NoCompactMarkVersion1, Details: "virtual block closing a shard compaction group"}
		}
	}
}

// splitJobGroup returns the group of a split job, with the settings and metrics of the stream's group.
func (g *SplitGrouper) splitJobGroup(job splitJobSpec) (*Group, error) {
	b := g.base
	res := metadata.ThanosDownsample{Resolution: downsample.ResLevel0}
	streamMeta := metadata.Thanos{Labels: job.streamLabels.Map(), Downsample: res}
	resolutionLabel := streamMeta.ResolutionString()
	shardID := job.shardID()
	key := fmt.Sprintf("%s_split_%s_%d-%d", streamMeta.GroupKey(), shardID, job.minTime, job.maxTime)

	jg, err := NewGroup(
		log.With(b.logger, "group", fmt.Sprintf("%s@%v", resolutionLabel, job.streamLabels.String()), "groupKey", key, "shard", shardID),
		b.bkt,
		key,
		job.streamLabels,
		res.Resolution,
		b.acceptMalformedIndex,
		b.enableVerticalCompaction,
		b.compactions.WithLabelValues(resolutionLabel),
		b.compactionRunsStarted.WithLabelValues(resolutionLabel),
		b.compactionRunsCompleted.WithLabelValues(resolutionLabel),
		b.compactionFailures.WithLabelValues(resolutionLabel),
		b.verticalCompactions.WithLabelValues(resolutionLabel),
		b.garbageCollectedBlocks,
		b.blocksMarkedForDeletion,
		b.blocksMarkedForNoCompact,
		b.hashFunc,
		b.blockFilesConcurrency,
		b.compactBlocksFetchConcurrency,
	)
	if err != nil {
		return nil, err
	}
	for _, m := range job.metas {
		if err := jg.AppendMeta(m); err != nil {
			return nil, err
		}
	}
	if err := jg.SetOutputLabels(labels.NewBuilder(job.streamLabels).Set(metadata.CompactorShardIDLabel, shardID).Labels()); err != nil {
		return nil, err
	}
	jg.SetPlanSingleBlock(true)
	jg.SetExtensions(&splitJob{index: job.index, scheme: job.scheme, inherited: commonExtensions(job.metas)})
	return jg, nil
}

// splitJob is the extensions of the group of a split job. In memory, it tells the wrappers of SplitGrouper which
// shard the group writes; written into the output's meta.json, it is the extensions the sources share plus the split
// scheme under metadata.CompactorSplitExtensionKey.
type splitJob struct {
	index     int
	scheme    metadata.SplitScheme
	inherited map[string]any
}

func (j *splitJob) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(j.inherited)+1)
	maps.Copy(m, j.inherited)
	m[metadata.CompactorSplitExtensionKey] = j.scheme
	return json.Marshal(m)
}

func splitJobOf(g *Group) (*splitJob, bool) {
	job, ok := g.Extensions().(*splitJob)
	return job, ok
}

// isShardGroup tells whether the group writes blocks of a shard: it compacts shard blocks further.
func isShardGroup(g *Group) bool {
	v := g.OutputLabels().Get(metadata.CompactorShardIDLabel)
	if v == "" {
		return false
	}
	_, _, err := metadata.ParseShardID(v)
	return err == nil
}

// commonExtensions returns the extensions all the blocks have, with the values of the first block, like
// DefaultGrouper does for its groups.
func commonExtensions(metas []*metadata.Meta) map[string]any {
	var combined map[string]any
	for _, m := range metas {
		ext, ok := m.Thanos.Extensions.(map[string]any)
		if !ok {
			continue
		}
		if combined == nil {
			combined = maps.Clone(ext)
			continue
		}
		for k := range combined {
			if _, ok := ext[k]; !ok {
				delete(combined, k)
			}
		}
	}
	return combined
}

func uniqueSortedSources(metas []*metadata.Meta) []ulid.ULID {
	var sources []ulid.ULID
	for _, m := range metas {
		sources = append(sources, m.Compaction.Sources...)
	}
	slices.SortFunc(sources, func(a, b ulid.ULID) int { return a.Compare(b) })
	return slices.Compact(sources)
}

// containsAllSorted tells whether every element of sub is in super; both must be sorted.
func containsAllSorted(super, sub []ulid.ULID) bool {
	for _, s := range sub {
		i, found := slices.BinarySearchFunc(super, s, func(a, b ulid.ULID) int { return a.Compare(b) })
		if !found {
			return false
		}
		super = super[i+1:]
	}
	return true
}

// Planner wraps the planner of the BucketCompactor: a split job is planned as all of its blocks, even a single one;
// other groups are planned by the wrapped planner. Split jobs are not subject to the planner filters (such as the
// index size limit): their output holds only a share of their sources' series.
func (g *SplitGrouper) Planner(p Planner) Planner {
	return splitPlanner{Planner: p}
}

type splitPlanner struct {
	Planner
}

func (p splitPlanner) Plan(ctx context.Context, metasByMinTime []*metadata.Meta, errChan chan error, extensions any) ([]*metadata.Meta, error) {
	if _, ok := extensions.(*splitJob); ok {
		return slices.Clone(metasByMinTime), nil
	}
	return p.Planner.Plan(ctx, metasByMinTime, errChan, extensions)
}

// BlockDeletableChecker wraps the checker of the BucketCompactor: the sources of a split job are never marked for
// deletion by the job. They are retired by the deduplication filter once every shard exists.
func (g *SplitGrouper) BlockDeletableChecker(c BlockDeletableChecker) BlockDeletableChecker {
	return splitBlockDeletableChecker{BlockDeletableChecker: c}
}

type splitBlockDeletableChecker struct {
	BlockDeletableChecker
}

func (c splitBlockDeletableChecker) CanDelete(group *Group, blockID ulid.ULID) bool {
	if _, ok := splitJobOf(group); ok {
		return false
	}
	return c.BlockDeletableChecker.CanDelete(group, blockID)
}

// CompactionLifecycleCallback wraps the callback of the BucketCompactor:
//   - a split job populates its block with PartitionedBlockPopulator;
//   - a group compacting shard blocks keeps empty outputs (see Compactor), and its output records the split scheme of
//     the blocks it compacts, which must all record the same one.
func (g *SplitGrouper) CompactionLifecycleCallback(c CompactionLifecycleCallback) CompactionLifecycleCallback {
	return splitLifecycleCallback{CompactionLifecycleCallback: c}
}

type splitLifecycleCallback struct {
	CompactionLifecycleCallback
}

func (c splitLifecycleCallback) PreCompactionCallback(ctx context.Context, logger log.Logger, group *Group, toCompactBlocks []*metadata.Meta) error {
	if err := c.CompactionLifecycleCallback.PreCompactionCallback(ctx, logger, group, toCompactBlocks); err != nil {
		return err
	}
	if _, ok := splitJobOf(group); ok || !isShardGroup(group) {
		return nil
	}
	return keepSplitScheme(group, toCompactBlocks)
}

// keepSplitScheme makes the output of a compaction of shard blocks record the split scheme its blocks record. The
// group's extensions combine those of all of its blocks, which may span several largest compaction ranges with
// different schemes; the blocks of one compaction always lie in one range and must record the same scheme.
func keepSplitScheme(group *Group, toCompact []*metadata.Meta) error {
	var common *metadata.SplitScheme
	for i, m := range toCompact {
		scheme, err := m.Thanos.SplitScheme()
		if err != nil {
			return halt(errors.Wrapf(err, "block %s", m.ULID))
		}
		if i == 0 {
			common = scheme
			continue
		}
		if (common == nil) != (scheme == nil) || (common != nil && !common.Equal(*scheme)) {
			return halt(errors.Errorf("shard blocks %s and %s record different split schemes (%v and %v): their series were not sharded the same way, refusing to compact them together",
				toCompact[0].ULID, m.ULID, common, scheme))
		}
	}

	current, _ := group.Extensions().(map[string]any)
	if _, has := current[metadata.CompactorSplitExtensionKey]; !has && common == nil {
		return nil
	}
	ext := maps.Clone(current)
	if ext == nil {
		ext = map[string]any{}
	}
	if common == nil {
		delete(ext, metadata.CompactorSplitExtensionKey)
	} else {
		ext[metadata.CompactorSplitExtensionKey] = *common
	}
	group.SetExtensions(ext)
	return nil
}

func (c splitLifecycleCallback) GetBlockPopulator(ctx context.Context, logger log.Logger, group *Group) (tsdb.BlockPopulator, error) {
	if job, ok := splitJobOf(group); ok {
		return PartitionedBlockPopulator{Shard: SeriesShardFor(job.scheme, job.index)}, nil
	}
	p, err := c.CompactionLifecycleCallback.GetBlockPopulator(ctx, logger, group)
	if err != nil || !isShardGroup(group) {
		return p, err
	}
	return keepEmptyPopulator{BlockPopulator: p}, nil
}

// emptyBlockKeeper is implemented by the populators of shard blocks: their output is written even when empty.
type emptyBlockKeeper interface {
	keepsEmptyBlock()
}

// keepEmptyPopulator is the populator of a group compacting shard blocks.
type keepEmptyPopulator struct {
	tsdb.BlockPopulator
}

func (keepEmptyPopulator) keepsEmptyBlock() {}

// Compactor wraps the compactor of the BucketCompactor so that a compaction of shard blocks that yields no series
// writes an empty block instead of none. Every shard of a split must have a block for the deduplication filter to
// retire the split's sources, including shards that hold none of their series; and the compactor would otherwise
// mark empty sources of a compaction for deletion without any block replacing them.
func (g *SplitGrouper) Compactor(c Compactor) Compactor {
	return splitCompactor{Compactor: c}
}

type splitCompactor struct {
	Compactor
}

func (c splitCompactor) CompactWithBlockPopulator(dest string, dirs []string, open []*tsdb.Block, blockPopulator tsdb.BlockPopulator) ([]ulid.ULID, error) {
	if _, ok := blockPopulator.(emptyBlockKeeper); !ok {
		return c.Compactor.CompactWithBlockPopulator(dest, dirs, open, blockPopulator)
	}
	// Read the sources' metas first: the compactor rewrites them when the result is empty.
	metas := make([]*tsdb.BlockMeta, 0, len(dirs))
	for _, dir := range dirs {
		m, err := readTSDBMeta(dir)
		if err != nil {
			return nil, err
		}
		metas = append(metas, m)
	}
	ids, err := c.Compactor.CompactWithBlockPopulator(dest, dirs, open, blockPopulator)
	if err != nil || len(ids) > 0 {
		return ids, err
	}
	id, err := writeEmptyBlock(dest, metas)
	if err != nil {
		return nil, errors.Wrap(err, "write empty shard block")
	}
	return []ulid.ULID{id}, nil
}

func readTSDBMeta(dir string) (*tsdb.BlockMeta, error) {
	b, err := os.ReadFile(filepath.Join(dir, metadata.MetaFilename))
	if err != nil {
		return nil, errors.Wrapf(err, "read meta of %s", dir)
	}
	var m tsdb.BlockMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, errors.Wrapf(err, "parse meta of %s", dir)
	}
	return &m, nil
}

// writeEmptyBlock writes, in dest, a block with no series that results from compacting blocks with the given metas:
// same time range, level, sources and parents as the compactor would give it.
func writeEmptyBlock(dest string, metas []*tsdb.BlockMeta) (ulid.ULID, error) {
	id := ulid.MustNew(ulid.Now(), rand.Reader)
	meta := tsdb.CompactBlockMetas(id, metas...)
	meta.Version = metadata.TSDBVersion1

	dir := filepath.Join(dest, id.String())
	tmp := dir + ".tmp-empty"
	if err := os.RemoveAll(tmp); err != nil {
		return id, err
	}
	if err := os.MkdirAll(filepath.Join(tmp, block.ChunksDirname), 0750); err != nil {
		return id, err
	}
	iw, err := index.NewWriter(context.Background(), filepath.Join(tmp, block.IndexFilename))
	if err != nil {
		return id, errors.Wrap(err, "open index writer")
	}
	if err := iw.Close(); err != nil {
		return id, errors.Wrap(err, "close index writer")
	}
	if _, err := tombstones.WriteFile(slog.New(slog.DiscardHandler), tmp, tombstones.NewMemTombstones()); err != nil {
		return id, errors.Wrap(err, "write tombstones")
	}
	b, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return id, err
	}
	if err := os.WriteFile(filepath.Join(tmp, metadata.MetaFilename), b, 0640); err != nil {
		return id, err
	}
	if err := os.Rename(tmp, dir); err != nil {
		return id, err
	}
	return id, nil
}

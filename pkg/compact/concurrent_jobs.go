// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// ConcurrentJobsGrouper splits every stream (the blocks of one group of the wrapped Grouper, by default the blocks
// with the same external labels and resolution) into compaction jobs that are independent of each other, so that
// BucketCompactor can compact many time ranges of a single stream at once instead of one plan per pass.
//
// The jobs of a pass are planned from the full, unmodified view of the stream, with the rules of the default planner
// (see tsdbBasedPlanner.plan):
//
//   - Blocks marked for no compaction are never part of a job.
//   - Each set of time-overlapping blocks (vertical compaction) is a job, planned first. Ranges are then planned on
//     the blocks these jobs produce, as the planner compacts ranges only once no blocks overlap.
//   - The newest block (max MinTime) is never part of a range job.
//   - For each compaction range in ascending order, an aligned range of blocks is a job when the default planner
//     would select it (at least two blocks, spanning the full range or ending before the second newest block starts,
//     no block whose compaction failed); blocks marked for no compaction split it into several jobs.
//   - An aligned range that overlaps in time a job already planned in this pass (vertical, or a smaller range) is
//     left for a later pass.
//
// Jobs therefore never share a block nor overlap in time, and each one is a compaction the default planner would
// also do, with the same blocks, if it ran one plan at a time. Planning the jobs again after removing planned blocks
// from the view would not have that property: the planner would see gaps where blocks are being compacted and could
// compact across them (see https://github.com/thanos-io/thanos/pull/3807). Here, the next pass simply syncs the
// bucket again and plans from the new view.
//
// Unlike the default planner, the jobs never include a rewrite of a single block with tombstones: blocks in object
// storage carry no tombstones file (block.Upload, used by the shipper and the compactor, does not upload it), so
// that rewrite would only produce a copy of the block with the same data.
//
// When vertical compaction is disabled and a stream has overlapping blocks, the whole stream is returned as a single
// group so that the compaction halts on the overlap check exactly like with the wrapped Grouper.
//
// Every job is a Group with only the blocks of that job and must be planned with NewConcurrentJobsPlanner. Job groups
// share the logger fields, metrics, extensions and settings of their stream's group.
type ConcurrentJobsGrouper struct {
	streams          Grouper
	ranges           []int64
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark
}

var _ Grouper = &ConcurrentJobsGrouper{}

// NewConcurrentJobsGrouper returns a ConcurrentJobsGrouper that splits the groups of the given Grouper (normally the
// DefaultGrouper) into compaction jobs. The ranges must be the ones the planner and compactor use.
func NewConcurrentJobsGrouper(streams Grouper, ranges []int64, noCompBlocks NoCompactMarks) *ConcurrentJobsGrouper {
	return &ConcurrentJobsGrouper{
		streams:          streams,
		ranges:           ranges,
		noCompBlocksFunc: noCompBlocks.NoCompactMarkedBlocks,
	}
}

// Groups returns one group per compaction job that can run in this pass.
func (g *ConcurrentJobsGrouper) Groups(blocks map[ulid.ULID]*metadata.Meta) ([]*Group, error) {
	streams, err := g.streams.Groups(blocks)
	if err != nil {
		return nil, err
	}

	noCompactMarked := g.noCompBlocksFunc()
	var res []*Group
	for _, stream := range streams {
		if !stream.enableVerticalCompaction && stream.areBlocksOverlapping(nil) != nil {
			// Keep the stream whole, so that its compaction fails the pre-compaction overlap check.
			res = append(res, stream)
			continue
		}

		jobs := planConcurrentJobs(g.ranges, noCompactMarked, sortedByMinTime(stream.metasByMinTime))
		if len(jobs) == 0 {
			continue
		}
		level.Info(stream.logger).Log("msg", "planned independent compaction jobs", "jobs", len(jobs))
		for _, job := range jobs {
			jg, err := stream.jobGroup(job)
			if err != nil {
				return nil, errors.Wrapf(err, "create compaction job group for %s", job)
			}
			level.Debug(jg.logger).Log("msg", "planned compaction job", "plan", job.String())
			res = append(res, jg)
		}
	}
	return res, nil
}

// jobGroup returns a group holding only the job's blocks, with the settings, metrics and extensions of the stream.
func (cg *Group) jobGroup(job *compactionJob) (*Group, error) {
	key := job.key(cg.key)
	g, err := NewGroup(
		log.With(cg.logger, "job", key),
		cg.bkt,
		key,
		cg.labels,
		cg.resolution,
		cg.acceptMalformedIndex,
		cg.enableVerticalCompaction,
		cg.compactions,
		cg.compactionRunsStarted,
		cg.compactionRunsCompleted,
		cg.compactionFailures,
		cg.verticalCompactions,
		cg.groupGarbageCollectedBlocks,
		cg.blocksMarkedForDeletion,
		cg.blocksMarkedForNoCompact,
		cg.hashFunc,
		cg.blockFilesConcurrency,
		cg.compactBlocksFetchConcurrency,
	)
	if err != nil {
		return nil, err
	}
	for _, m := range job.metas {
		if err := g.AppendMeta(m); err != nil {
			return nil, err
		}
	}
	g.SetExtensions(cg.Extensions())
	return g, nil
}

// compactionJob is one compaction of a stream that can run concurrently with the other jobs of the same pass.
type compactionJob struct {
	// rangeLength is the compaction range of a range job, or 0 for a vertical compaction job.
	rangeLength int64
	// minTime and maxTime bound the time range [minTime, maxTime) no other job of the pass may overlap: the aligned
	// range for a range job, the time span of the blocks for a vertical compaction job.
	minTime, maxTime int64
	// metas are the blocks to compact, sorted by MinTime.
	metas []*metadata.Meta
}

func (j *compactionJob) conflicts(other *compactionJob) bool {
	return j.minTime < other.maxTime && other.minTime < j.maxTime
}

// key returns a filesystem-safe group key, unique within a pass: jobs of a stream never share blocks.
func (j *compactionJob) key(streamKey string) string {
	kind := "vertical"
	if j.rangeLength > 0 {
		kind = model.Duration(time.Duration(j.rangeLength) * time.Millisecond).String()
	}
	mint, maxt := blocksTimeSpan(j.metas)
	return fmt.Sprintf("%s_%s_%d-%d", streamKey, kind, mint, maxt)
}

func (j *compactionJob) String() string {
	ids := make([]string, 0, len(j.metas))
	for _, m := range j.metas {
		ids = append(ids, m.ULID.String())
	}
	if j.rangeLength == 0 {
		return fmt.Sprintf("vertical compaction of [%d, %d): %s", j.minTime, j.maxTime, strings.Join(ids, ","))
	}
	return fmt.Sprintf("compaction of range %s [%d, %d): %s",
		model.Duration(time.Duration(j.rangeLength)*time.Millisecond), j.minTime, j.maxTime, strings.Join(ids, ","))
}

// planConcurrentJobs returns the compaction jobs of one stream for a single pass. It applies the rules of
// tsdbBasedPlanner.plan to all of the stream at once instead of returning only the first plan; see
// ConcurrentJobsGrouper. The metas must be sorted by MinTime.
func planConcurrentJobs(ranges []int64, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark, metasByMinTime []*metadata.Meta) []*compactionJob {
	if len(metasByMinTime) == 0 {
		return nil
	}

	notExcluded := make([]*metadata.Meta, 0, len(metasByMinTime))
	for _, m := range metasByMinTime {
		if _, excluded := noCompactMarked[m.ULID]; !excluded {
			notExcluded = append(notExcluded, m)
		}
	}

	// The planner compacts overlapping blocks before anything else, one set at a time. Merging a set produces a block
	// spanning exactly the set, which overlaps nothing else, so every set is a job of this pass.
	var (
		jobs   []*compactionJob
		view   = metasByMinTime
		merged = map[*metadata.Meta]struct{}{}
	)
	if sets := overlappingSets(notExcluded); len(sets) > 0 {
		// The planner compacts ranges only once no blocks overlap, so it plans them on the blocks these jobs produce.
		// Plan the ranges on that view too: merging a set can change the most recent blocks, and so which ranges are
		// complete. A range holding a merged block overlaps its job and is left to a later pass.
		inSet := map[ulid.ULID]struct{}{}
		view = make([]*metadata.Meta, 0, len(metasByMinTime))
		for _, set := range sets {
			mint, maxt := blocksTimeSpan(set)
			jobs = append(jobs, &compactionJob{minTime: mint, maxTime: maxt, metas: set})

			m := &metadata.Meta{BlockMeta: tsdb.BlockMeta{MinTime: mint, MaxTime: maxt}}
			merged[m] = struct{}{}
			view = append(view, m)
			for _, m := range set {
				inSet[m.ULID] = struct{}{}
			}
		}
		for _, m := range metasByMinTime {
			if _, ok := inSet[m.ULID]; !ok {
				view = append(view, m)
			}
		}
		view = sortedByMinTime(view)
	}

	// Same as the planner: the most recent block is never compacted with others, so that a block of the same size
	// still fits in its range once uploaded. This holds even when it is marked for no compaction.
	metas := view[:len(view)-1]
	if len(ranges) < 2 || len(metas) == 0 {
		return jobs
	}
	highTime := metas[len(metas)-1].MinTime

	for _, tr := range ranges[1:] {
	nextRange:
		for _, p := range splitByRange(metas, tr) {
			// The conditions of selectMetas.
			for _, m := range p {
				if m.Compaction.Failed {
					continue nextRange
				}
				if _, ok := merged[m]; ok {
					continue nextRange
				}
			}
			if len(p) < 2 {
				continue
			}
			if maxt := p[len(p)-1].MaxTime; maxt-p[0].MinTime != tr && maxt > highTime {
				continue
			}

			// Leave the range to a later pass if a job of this pass compacts any of its time range: the planner would
			// do that job first, and the result may change what this range holds.
			t0 := alignedRangeStart(p[0].MinTime, tr)
			candidate := &compactionJob{rangeLength: tr, minTime: t0, maxTime: t0 + tr}
			for _, j := range jobs {
				if candidate.conflicts(j) {
					continue nextRange
				}
			}

			// Blocks marked for no compaction split the range into runs of blocks that the planner compacts one after
			// the other; compacting a run does not change the others, so each one is a job.
			for _, run := range runsWithoutNoCompact(p, noCompactMarked) {
				jobs = append(jobs, &compactionJob{rangeLength: tr, minTime: t0, maxTime: t0 + tr, metas: run})
			}
		}
	}
	return jobs
}

// overlappingSets returns every set of time-overlapping blocks with at least two blocks. The first one is the set
// selectOverlappingMetas returns. The metas must be sorted by MinTime.
func overlappingSets(metasByMinTime []*metadata.Meta) [][]*metadata.Meta {
	if len(metasByMinTime) < 2 {
		return nil
	}
	var (
		sets       [][]*metadata.Meta
		set        = []*metadata.Meta{metasByMinTime[0]}
		globalMaxt = metasByMinTime[0].MaxTime
	)
	for _, m := range metasByMinTime[1:] {
		if m.MinTime >= globalMaxt {
			if len(set) > 1 {
				sets = append(sets, set)
			}
			set = nil
		}
		set = append(set, m)
		if m.MaxTime > globalMaxt {
			globalMaxt = m.MaxTime
		}
	}
	if len(set) > 1 {
		sets = append(sets, set)
	}
	return sets
}

// runsWithoutNoCompact returns the runs of at least two consecutive blocks not marked for no compaction, in the
// order selectMetas returns them.
func runsWithoutNoCompact(metas []*metadata.Meta, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark) [][]*metadata.Meta {
	var runs [][]*metadata.Meta
	lastExcluded := 0
	for i, m := range metas {
		if _, excluded := noCompactMarked[m.ULID]; !excluded {
			continue
		}
		if i-lastExcluded > 1 {
			runs = append(runs, metas[lastExcluded:i])
		}
		lastExcluded = i + 1
	}
	if len(metas)-lastExcluded > 1 {
		runs = append(runs, metas[lastExcluded:])
	}
	return runs
}

func blocksTimeSpan(metas []*metadata.Meta) (mint, maxt int64) {
	mint, maxt = metas[0].MinTime, metas[0].MaxTime
	for _, m := range metas[1:] {
		mint = min(mint, m.MinTime)
		maxt = max(maxt, m.MaxTime)
	}
	return mint, maxt
}

// sortedByMinTime returns a copy of the metas sorted by MinTime. Ties are ordered by MaxTime and ULID, so that the
// jobs do not depend on the order the blocks were synced in.
func sortedByMinTime(metas []*metadata.Meta) []*metadata.Meta {
	res := append([]*metadata.Meta(nil), metas...)
	sort.Slice(res, func(i, j int) bool {
		if res[i].MinTime != res[j].MinTime {
			return res[i].MinTime < res[j].MinTime
		}
		if res[i].MaxTime != res[j].MaxTime {
			return res[i].MaxTime < res[j].MaxTime
		}
		return res[i].ULID.Compare(res[j].ULID) < 0
	})
	return res
}

// concurrentJobsPlanner plans the groups of ConcurrentJobsGrouper: the plan of a job group is all of its blocks.
type concurrentJobsPlanner struct {
	logger           log.Logger
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark
}

var _ noCompactAwarePlanner = &concurrentJobsPlanner{}

// NewConcurrentJobsPlanner returns the planner for the groups of ConcurrentJobsGrouper: the plan of a job group is
// the job, that is all of its blocks. Wrap it with WithLargeTotalIndexSizeFilter (and
// WithVerticalCompactionDownsampleFilter with vertical compaction) like the default planner. When a filter marks
// one of the job's blocks for no compaction, the plan is what is left of the job the way the default planner would
// split it: the first remaining set of overlapping blocks for a vertical compaction job, the first run of at least
// two blocks otherwise. The rest is planned again in the next pass.
// It must not be used with groups of other groupers: it would compact all of their blocks at once.
func NewConcurrentJobsPlanner(logger log.Logger, noCompBlocks NoCompactMarks) *concurrentJobsPlanner {
	return &concurrentJobsPlanner{logger: logger, noCompBlocksFunc: noCompBlocks.NoCompactMarkedBlocks}
}

func (p *concurrentJobsPlanner) Plan(_ context.Context, metasByMinTime []*metadata.Meta, _ chan error, _ any) ([]*metadata.Meta, error) {
	return p.plan(p.noCompBlocksFunc(), metasByMinTime)
}

func (p *concurrentJobsPlanner) plan(noCompactMarked map[ulid.ULID]*metadata.NoCompactMark, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	if len(selectOverlappingMetas(metasByMinTime)) > 0 {
		// A vertical compaction job.
		notExcluded := make([]*metadata.Meta, 0, len(metasByMinTime))
		for _, m := range metasByMinTime {
			if _, excluded := noCompactMarked[m.ULID]; !excluded {
				notExcluded = append(notExcluded, m)
			}
		}
		return selectOverlappingMetas(notExcluded), nil
	}

	// A range job: consecutive blocks of one aligned range.
	if runs := runsWithoutNoCompact(metasByMinTime, noCompactMarked); len(runs) > 0 {
		return append([]*metadata.Meta(nil), runs[0]...), nil
	}
	return nil, nil
}

func (p *concurrentJobsPlanner) noCompactMarkedBlocks() map[ulid.ULID]*metadata.NoCompactMark {
	return p.noCompBlocksFunc()
}

func (p *concurrentJobsPlanner) plannerLogger() log.Logger {
	return p.logger
}

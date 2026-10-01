// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"cmp"
	"slices"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
)

// The time-only selection of getForTimeRange reads a finer block only where
// no coarser block the request may use exists. A coarser block can overlap a
// finer one in time and still lack some of its data, e.g. when a raw block
// gained sources through a later compaction, backfill or vertical compaction
// after its 5m block was made. That data would never be read.
//
// selectUncovered therefore also selects every finer block the request may
// use, unless the coarser blocks the request may use hold all its sources
// over its whole part of the request. The series merge and the querier's
// iterators handle the chunks that both blocks then return.
//
// What the check needs is kept per block and updated when blocks are added
// or removed, so getFor builds nothing from the sources per request. A
// request whose finer blocks overlap no coarser block it may use costs what
// the time-only selection costs. Each finer block that overlaps one costs a
// few comparisons. Only with block matchers, or when the coarser blocks hold
// all sources of a finer block but not over its whole time range, getFor
// walks the coarser blocks overlapping it for the request's range, and looks
// its sources up in them only if none of them holds all its sources.

// blockLineage is what the selection knows about one block of a set.
type blockLineage struct {
	// level is the index of the block's resolution in bucketBlockSet.resolutions.
	// A larger index is a finer resolution.
	level int
	// sources are the block's compaction sources, sorted and unique.
	sources []ulid.ULID
	// coarser holds the blocks of coarser resolutions that overlap the block in
	// time, ordered by min time, then max time.
	coarser []coarserBlock

	// finestCoarser is the largest level in coarser, or -1 if coarser is empty.
	finestCoarser int
	// heldFrom is the largest level k such that the blocks in coarser with
	// level k or larger hold every source of the block, or -1.
	heldFrom int
	// coveredFrom is the largest level k such that the blocks in coarser with
	// level k or larger hold every source of the block over its whole time
	// range, or -1.
	coveredFrom int
}

// coarserBlock is a block overlapping a finer block in time.
type coarserBlock struct {
	block *bucketBlock
	level int // As blockLineage.level.
	// held has bit i set if the block holds sources[i] of the finer block. It
	// is nil if the block holds them all.
	held []uint64
}

// holds reports whether the block holds sources[i] of the finer block.
func (c coarserBlock) holds(i int) bool {
	return c.held == nil || c.held[i/64]&(1<<(i%64)) != 0
}

// selectUncovered appends to bs, the time-only selection of the request,
// each finer block the request may use, that it does not hold yet and whose
// sources the coarser blocks the request may use do not all hold over its
// part of the range. Block matchers apply before a block counts as coarser
// data. It never removes a block from bs and must be called under s.mtx.
func (s *bucketBlockSet) selectUncovered(bs []*bucketBlock, mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) []*bucketBlock {
	// The coarsest level the request may use, as in getForTimeRange.
	level := 0
	for ; level < len(s.resolutions) && s.resolutions[level] > maxResolutionMillis; level++ {
	}

	timeOnly := len(bs)
	for j := level + 1; j < len(s.resolutions); j++ {
		if !s.anyOverlapped(j, level) {
			// Every block of this level lies in time gaps of the levels the
			// request may use, where the time-only selection took it.
			continue
		}
		for _, b := range s.blocks[j] {
			if b.meta.MaxTime <= mint {
				continue
			}
			if b.meta.MinTime > maxt {
				break
			}
			lin := s.lineages[b]
			if lin.finestCoarser < level {
				// In a time gap of every level the request may use.
				continue
			}
			if len(blockMatchers) == 0 {
				if lin.coveredFrom >= level {
					continue
				}
			} else if !b.matchRelabelLabels(blockMatchers) {
				continue
			}
			if lin.heldFrom >= level && lin.covers(level, max(mint, b.meta.MinTime), min(maxt, b.meta.MaxTime-1), blockMatchers) {
				continue
			}
			// A block partly in a time gap is uncovered, but selected already.
			if slices.Contains(bs[:timeOnly], b) {
				continue
			}
			bs = append(bs, b)
		}
	}
	return bs
}

// anyOverlapped reports whether a block of the given level overlaps in time a
// block of a coarser level that is not coarser than minLevel.
func (s *bucketBlockSet) anyOverlapped(level, minLevel int) bool {
	if level >= len(s.overlapped) {
		// A set without lineages, as tests build them.
		return false
	}
	for k := minLevel; k < level; k++ {
		if s.overlapped[level][k] > 0 {
			return true
		}
	}
	return false
}

// covers reports whether the blocks in coarser with the given level or a
// larger one, which match the block matchers, hold every source of the block
// over [mint, maxt].
//
// A coarser block that the request may use but was not selected stands in
// for the selected blocks: selectUncovered checks levels from coarse to fine,
// so such a block is covered by them over its own time range.
func (lin *blockLineage) covers(level int, mint, maxt int64, blockMatchers []*labels.Matcher) bool {
	if mint > maxt {
		return true
	}
	var buf [8]coarserBlock
	candidates := buf[:0]
	for _, c := range lin.coarser {
		if c.block.meta.MinTime > maxt {
			break
		}
		if c.block.meta.MaxTime <= mint || c.level < level {
			continue
		}
		if len(blockMatchers) > 0 && !c.block.matchRelabelLabels(blockMatchers) {
			continue
		}
		candidates = append(candidates, c)
	}

	if spans(candidates, mint, maxt, func(c coarserBlock) bool { return c.held == nil }) {
		return true
	}
	// Where the candidates leave a time gap, no source is covered.
	if len(lin.sources) == 0 || !spans(candidates, mint, maxt, func(coarserBlock) bool { return true }) {
		return false
	}
	// The candidates hold the sources only together.
	for i := range lin.sources {
		if !spans(candidates, mint, maxt, func(c coarserBlock) bool { return c.holds(i) }) {
			return false
		}
	}
	return true
}

// spans reports whether the candidates for which use returns true cover
// [mint, maxt] together. The candidates must be ordered by min time.
func spans(candidates []coarserBlock, mint, maxt int64, use func(coarserBlock) bool) bool {
	for _, c := range candidates {
		if c.block.meta.MinTime > mint {
			return false
		}
		if c.block.meta.MaxTime <= mint || !use(c) {
			continue
		}
		mint = c.block.meta.MaxTime
		if mint > maxt {
			return true
		}
	}
	return false
}

// computeHeldFrom returns heldFrom from coarser.
func (lin *blockLineage) computeHeldFrom() int {
	from := lin.finestCoarser
	if from < 0 || len(lin.sources) == 0 {
		return from
	}
	for _, c := range lin.coarser {
		if c.held == nil && c.level == from {
			return from
		}
	}
	for i := range lin.sources {
		finest := -1
		for _, c := range lin.coarser {
			if c.level > finest && c.holds(i) {
				finest = c.level
			}
		}
		from = min(from, finest)
		if from < 0 {
			break
		}
	}
	return from
}

// addLineage records the lineage of a block added at the given level and
// updates the finer blocks it overlaps. It must be called under s.mtx.
func (s *bucketBlockSet) addLineage(b *bucketBlock, level int) {
	if s.lineages == nil {
		s.initLineages()
	}
	if old, ok := s.lineages[b]; ok {
		// The same block added twice keeps one lineage.
		s.countOverlapped(old, -1)
	}
	s.lineages[b] = &blockLineage{level: level, sources: sortedSources(b.meta.Compaction.Sources), finestCoarser: -1}
	s.refreshLineage(b)
	s.refreshFinerLineages(b, level)
}

func (s *bucketBlockSet) initLineages() {
	s.lineages = map[*bucketBlock]*blockLineage{}
	s.overlapped = make([][]int, len(s.resolutions))
	for i := range s.overlapped {
		s.overlapped[i] = make([]int, len(s.resolutions))
	}
}

// removeLineage drops the lineage of a block removed from the given level and
// updates the finer blocks it overlapped. It must be called under s.mtx.
func (s *bucketBlockSet) removeLineage(b *bucketBlock, level int) {
	lin, ok := s.lineages[b]
	if !ok {
		return
	}
	if slices.Contains(s.blocks[level], b) {
		// The same block was added twice and is still in the set.
		return
	}
	s.countOverlapped(lin, -1)
	delete(s.lineages, b)
	s.refreshFinerLineages(b, level)
}

// refreshFinerLineages refreshes the lineage of every block finer than the
// given level that overlaps b in time.
func (s *bucketBlockSet) refreshFinerLineages(b *bucketBlock, level int) {
	for j := level + 1; j < len(s.blocks); j++ {
		for _, f := range s.blocks[j] {
			if f.meta.MinTime >= b.meta.MaxTime {
				break
			}
			if f.meta.MaxTime > b.meta.MinTime {
				s.refreshLineage(f)
			}
		}
	}
}

// refreshLineage recomputes what the lineage of b derives from the coarser
// blocks of the set.
func (s *bucketBlockSet) refreshLineage(b *bucketBlock) {
	lin := s.lineages[b]
	s.countOverlapped(lin, -1)

	lin.coarser = lin.coarser[:0]
	lin.finestCoarser = -1
	for k := 0; k < lin.level; k++ {
		for _, c := range s.blocks[k] {
			if c.meta.MinTime >= b.meta.MaxTime {
				break
			}
			if c.meta.MaxTime <= b.meta.MinTime {
				continue
			}
			lin.coarser = append(lin.coarser, coarserBlock{block: c, level: k, held: heldSources(s.lineages[c].sources, lin.sources)})
			lin.finestCoarser = k
		}
	}
	slices.SortStableFunc(lin.coarser, func(x, y coarserBlock) int {
		return cmp.Or(cmp.Compare(x.block.meta.MinTime, y.block.meta.MinTime), cmp.Compare(x.block.meta.MaxTime, y.block.meta.MaxTime))
	})

	lin.heldFrom, lin.coveredFrom = lin.computeHeldFrom(), -1
	for k := lin.heldFrom; k >= 0; k-- {
		if lin.covers(k, b.meta.MinTime, b.meta.MaxTime-1, nil) {
			lin.coveredFrom = k
			break
		}
	}
	s.countOverlapped(lin, 1)
}

func (s *bucketBlockSet) countOverlapped(lin *blockLineage, delta int) {
	if lin.finestCoarser >= 0 {
		s.overlapped[lin.level][lin.finestCoarser] += delta
	}
}

// sortedSources returns the sources sorted and without duplicates. Compaction
// writes them so, and then they are returned as they are.
func sortedSources(sources []ulid.ULID) []ulid.ULID {
	for i := 1; i < len(sources); i++ {
		if sources[i-1].Compare(sources[i]) >= 0 {
			sorted := slices.Clone(sources)
			slices.SortFunc(sorted, ulid.ULID.Compare)
			return slices.Compact(sorted)
		}
	}
	return sources
}

// heldSources returns a bitset of the finer block's sources that the coarser
// block holds, or nil if it holds them all. Both must be sorted and unique.
func heldSources(coarser, finer []ulid.ULID) []uint64 {
	var held []uint64
	j := 0
	for i, src := range finer {
		for j < len(coarser) && coarser[j].Compare(src) < 0 {
			j++
		}
		if j < len(coarser) && coarser[j] == src {
			j++
			if held != nil {
				held[i/64] |= 1 << (i % 64)
			}
			continue
		}
		if held == nil {
			// The first source the coarser block lacks; it holds all before.
			held = make([]uint64, (len(finer)+63)/64)
			for k := range i {
				held[k/64] |= 1 << (k % 64)
			}
		}
	}
	return held
}

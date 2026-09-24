// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// TestShardTreeNeverLosesCoverage drives the deduplication filter, as a
// compactor uses it, through random histories of a block stream: splits that
// may fail after any number of shards, re-splits of shards, horizontal merges
// of what the view shows, and garbage collection of what the filter calls a
// duplicate. After every step, every series of every original block must
// still be held by some block in the bucket and by some block in the view,
// with the series modeled as the residues of the hash space each shard
// holds. This is the property the publication set, the lineage rules and the
// withholding of unpublished blocks exist for; a history that breaks it is
// printed so that it can be turned into a fixed test.
func TestShardTreeNeverLosesCoverage(t *testing.T) {
	const (
		ranges   = 4
		residues = 16 // The finest count a shard may reach.
		steps    = 40
	)
	for seed := int64(1); seed <= 150; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			s := newShardTreeSim(seed, ranges, residues)
			for range steps {
				s.step()
				s.sync(t)
				s.check(t)
				if t.Failed() {
					t.Logf("history:\n%s", s.history())
					t.FailNow()
				}
			}
		})
	}
}

// simBlock is a block in the simulated bucket.
type simBlock struct {
	id      ulid.ULID
	index   uint64 // Shard index, count zero for an unsplit block.
	count   uint64
	mint    int64 // Ranges are unit intervals.
	maxt    int64
	sources []ulid.ULID
	set     []ulid.ULID // The plan's output set, nil for a block of its own.
	deleted bool
}

// holds says whether the block holds residue j of the hash space.
func (b *simBlock) holds(j uint64) bool { return b.count == 0 || j%b.count == b.index }

func (b *simBlock) label() string {
	if b.count == 0 {
		return "unsplit"
	}
	return fmt.Sprintf("%d_of_%d", b.index+1, b.count)
}

type shardTreeSim struct {
	rnd      *rand.Rand
	residues uint64
	next     int
	blocks   []*simBlock
	view     []*simBlock
	original map[ulid.ULID]*simBlock // The corpus: one unsplit block per range.
	log      []string
}

func newShardTreeSim(seed int64, ranges int, residues uint64) *shardTreeSim {
	s := &shardTreeSim{rnd: rand.New(rand.NewSource(seed)), residues: residues, original: map[ulid.ULID]*simBlock{}}
	for r := range ranges {
		b := s.add(0, 0, int64(r), int64(r)+1, nil, nil)
		s.original[b.id] = b
	}
	s.view = slices.Clone(s.blocks)
	return s
}

func (s *shardTreeSim) add(index, count uint64, mint, maxt int64, sources, set []ulid.ULID) *simBlock {
	s.next++
	id := ulid.MustNew(uint64(s.next), nil)
	if sources == nil {
		sources = []ulid.ULID{id}
	}
	b := &simBlock{id: id, index: index, count: count, mint: mint, maxt: maxt, sources: sources, set: set}
	s.blocks = append(s.blocks, b)
	return b
}

func (s *shardTreeSim) history() string {
	var out string
	for _, l := range s.log {
		out += "  " + l + "\n"
	}
	out += "  bucket:\n"
	for _, b := range s.blocks {
		state := "live"
		if b.deleted {
			state = "deleted"
		} else if slices.Contains(s.view, b) {
			state = "in view"
		}
		out += fmt.Sprintf("    %s %s [%d,%d) sources=%v set=%v %s\n", b.id, b.label(), b.mint, b.maxt, b.sources, b.set, state)
	}
	return out
}

// step performs one random operation on what the view shows, as a compactor
// would: it plans only among visible blocks.
func (s *shardTreeSim) step() {
	if len(s.view) == 0 {
		return
	}
	switch s.rnd.Intn(3) {
	case 0, 1:
		s.split()
	case 2:
		s.merge()
	}
}

// split re-partitions the visible blocks of one shard (or the unsplit
// blocks) of one range into k times as many shards, uploading only the first
// m of them when the attempt fails.
func (s *shardTreeSim) split() {
	b := s.view[s.rnd.Intn(len(s.view))]
	// Everything visible with the same shard and range goes into the plan,
	// as overlapping replicas would.
	var plan []*simBlock
	for _, o := range s.view {
		if o.index == b.index && o.count == b.count && o.mint == b.mint && o.maxt == b.maxt {
			plan = append(plan, o)
		}
	}
	k := uint64(2)
	if s.rnd.Intn(3) == 0 {
		k = 4
	}
	count := max(b.count, 1) * k
	if count > s.residues {
		return
	}
	var sources []ulid.ULID
	for _, p := range plan {
		sources = append(sources, p.sources...)
	}
	slices.SortFunc(sources, func(a, b ulid.ULID) int { return a.Compare(b) })
	sources = slices.Compact(sources)
	// Output indexes: the shards congruent to the source shard.
	var idx []uint64
	for j := b.index; j < count; j += max(b.count, 1) {
		idx = append(idx, j)
	}
	uploaded := len(idx)
	if s.rnd.Intn(3) == 0 {
		uploaded = s.rnd.Intn(len(idx)) // Fails after this many shards.
	}
	// The set is known before the first upload, and names the siblings:
	// the visible shards disjoint from the source over the span.
	set := make([]ulid.ULID, 0, len(idx))
	for range idx {
		s.next++
		set = append(set, ulid.MustNew(uint64(s.next), nil))
	}
	set = append(set, s.siblings(plan)...)
	for i, j := range idx[:uploaded] {
		s.blocks = append(s.blocks, &simBlock{id: set[i], index: j, count: count, mint: b.mint, maxt: b.maxt, sources: sources, set: set})
	}
	s.log = append(s.log, fmt.Sprintf("split %s [%d,%d) into %d shards of %d, %d uploaded", b.label(), b.mint, b.maxt, len(idx), count, uploaded))
}

// siblings names, as the planner does, every visible block that shares a set
// with one of the plan's sources: the sources' own sets and the sets of other
// visible blocks that name a source.
func (s *shardTreeSim) siblings(plan []*simBlock) []ulid.ULID {
	isSource := map[ulid.ULID]bool{}
	for _, p := range plan {
		isSource[p.id] = true
	}
	inView := map[ulid.ULID]bool{}
	for _, o := range s.view {
		inView[o.id] = true
	}
	named := map[ulid.ULID]bool{}
	collect := func(set []ulid.ULID) {
		for _, id := range set {
			if !isSource[id] && inView[id] {
				named[id] = true
			}
		}
	}
	for _, p := range plan {
		collect(p.set)
	}
	for _, o := range s.view {
		if slices.ContainsFunc(o.set, func(id ulid.ULID) bool { return isSource[id] }) {
			collect(o.set)
		}
	}
	return slices.SortedFunc(maps.Keys(named), func(a, b ulid.ULID) int { return a.Compare(b) })
}

// merge compacts two visible, adjacent blocks of the same shard into one, as
// a horizontal compaction does: no set of its own.
func (s *shardTreeSim) merge() {
	for _, a := range s.view {
		for _, b := range s.view {
			if a.index == b.index && a.count == b.count && a.maxt == b.mint {
				sources := slices.Clone(a.sources)
				sources = append(sources, b.sources...)
				slices.SortFunc(sources, func(x, y ulid.ULID) int { return x.Compare(y) })
				sources = slices.Compact(sources)
				siblings := s.siblings([]*simBlock{a, b})
				nb := s.add(a.index, a.count, a.mint, b.maxt, sources, nil)
				if len(siblings) > 0 {
					nb.set = append([]ulid.ULID{nb.id}, siblings...)
				}
				s.log = append(s.log, fmt.Sprintf("merge %s [%d,%d)+[%d,%d)", a.label(), a.mint, a.maxt, b.mint, b.maxt))
				return
			}
		}
	}
}

// sync runs the real filter over the bucket, as a compactor's sync does,
// deletes what it calls a duplicate, and remembers the view.
func (s *shardTreeSim) sync(t *testing.T) {
	t.Helper()
	metas := map[ulid.ULID]*metadata.Meta{}
	byID := map[ulid.ULID]*simBlock{}
	for _, b := range s.blocks {
		if b.deleted {
			continue
		}
		lbls := map[string]string{"tenant": "a"}
		if b.count > 0 {
			lbls[metadata.CompactorShardLabel] = b.label()
		}
		m := &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: b.id, MinTime: b.mint, MaxTime: b.maxt, Compaction: tsdb.BlockMetaCompaction{Sources: b.sources}},
			Thanos:    metadata.Thanos{Labels: lbls},
		}
		if b.set != nil {
			m.Thanos.Output = &metadata.ThanosOutput{Index: int(slices.Index(b.set, b.id)), Count: len(b.set), Blocks: b.set}
		}
		metas[b.id] = m
		byID[b.id] = b
	}
	f := NewDeduplicateFilter(1)
	f.HideUnpublished()
	testutil.Ok(t, f.Filter(t.Context(), metas, newTestFetcherMetrics().Synced, nil))
	for _, id := range f.DuplicateIDs() {
		byID[id].deleted = true
	}
	s.view = s.view[:0]
	for id := range metas {
		s.view = append(s.view, byID[id])
	}
	slices.SortFunc(s.view, func(a, b *simBlock) int { return a.id.Compare(b.id) })
}

// check asserts that every residue of every original block's range is held
// by a live block descending from it, in the bucket and in the view.
func (s *shardTreeSim) check(t *testing.T) {
	t.Helper()
	for id, o := range s.original {
		for j := range s.residues {
			bucket, view := false, false
			for _, b := range s.blocks {
				if b.deleted || !slices.Contains(b.sources, id) || !b.holds(j) || b.mint > o.mint || b.maxt < o.maxt {
					continue
				}
				bucket = true
				if slices.Contains(s.view, b) {
					view = true
				}
			}
			if !bucket {
				t.Errorf("residue %d of range [%d,%d) is held by no block in the bucket", j, o.mint, o.maxt)
				return
			}
			if !view {
				t.Errorf("residue %d of range [%d,%d) is held by no block in the view", j, o.mint, o.maxt)
				return
			}
		}
	}
}

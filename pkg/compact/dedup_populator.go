// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"cmp"
	"container/heap"
	"context"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"
)

// SeriesDedupStats is the tally of one deduplicating compaction pass.
type SeriesDedupStats struct {
	// Input counts the series of the sources the pass took in - within its
	// partition, if it has one - once per source block holding them.
	Input uint64
	// Output counts the series it wrote.
	Output uint64
}

// SeriesDedupMetrics count what deduplication of series replicas does.
type SeriesDedupMetrics struct {
	Input  prometheus.Counter
	Output prometheus.Counter
}

// NewSeriesDedupMetrics registers the metrics of series deduplication.
func NewSeriesDedupMetrics(reg prometheus.Registerer) *SeriesDedupMetrics {
	return &SeriesDedupMetrics{
		Input: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_series_dedup_input_series_total",
			Help: "Series read by compactions that deduplicate series replicas, counted once per source block holding them.",
		}),
		Output: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_series_dedup_output_series_total",
			Help: "Series written by compactions that deduplicate series replicas.",
		}),
	}
}

// NormalizeSeriesReplicaLabels returns the label names sorted, without
// duplicates or empty names.
func NormalizeSeriesReplicaLabels(names []string) []string {
	out := slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == "" })
	slices.Sort(out)
	return slices.Compact(out)
}

// DeduplicatingBlockPopulator is a tsdb.BlockPopulator that merges series
// which differ only in the replica labels it is given, drops those labels
// from what it writes, and otherwise does what tsdb.DefaultBlockPopulator
// does. It is for HA replicas whose replica label sits inside the series -
// Prometheus pairs behind a receiver, agents over remote write, collector
// pairs - which external-label deduplication cannot see.
//
// Series are merged with the compactor's merge function, offered in a fixed
// order: by source block - MinTime, then ULID - and, within a source, by
// their replica labels compared as a label set, a series lacking them first.
// The penalty merger keeps the samples of the chunk it merges the others
// into, and gives ties between chunks with the same time range to the series
// offered first, so a fixed order is what makes the output depend on the
// sources alone and not on how a heap happened to settle. The merger runs
// the penalty algorithm on each group of overlapping chunks on its own,
// starting afresh - a group spans about one chunk, 120 samples, half an hour
// at a 15s scrape - so each group holds what a querier reading that group
// alone returns. A querier reading across groups carries its state on and
// can differ at the group boundaries: for replicas scraped at an offset, and
// for replicas in lockstep once one of them has a gap. Every sample kept is
// one a replica really has.
//
// Optionally the populator writes one partition of the series only, like
// PartitionedBlockPopulator; the partition must then leave the replica labels
// out of its hash, or replicas of one series would fall into different
// partitions and never meet.
type DeduplicatingBlockPopulator struct {
	// ReplicaLabels are the series labels to deduplicate by, as returned by
	// NormalizeSeriesReplicaLabels.
	ReplicaLabels []string
	// Partition, if set, restricts the output to one partition.
	Partition *SeriesPartition
	// PartitionStats, if set, receives the partition tally; see
	// PartitionedBlockPopulator.
	PartitionStats *PartitionStats
	// Stats, if set, receives the deduplication tally.
	Stats *SeriesDedupStats
}

var _ tsdb.BlockPopulator = DeduplicatingBlockPopulator{}

func (p DeduplicatingBlockPopulator) validate() error {
	if len(p.ReplicaLabels) == 0 {
		return errors.New("deduplicating populator needs at least one replica label")
	}
	if !slices.Equal(p.ReplicaLabels, NormalizeSeriesReplicaLabels(p.ReplicaLabels)) {
		return errors.Errorf("replica labels %v are not normalized", p.ReplicaLabels)
	}
	if p.Partition == nil {
		return nil
	}
	if err := p.Partition.Validate(); err != nil {
		return errors.Wrap(err, "validate series partition")
	}
	for _, name := range p.ReplicaLabels {
		if !slices.Contains(p.Partition.Without, name) {
			return errors.Errorf("series partition %d of %d hashes the series replica label %q, so replicas of one series would land in different partitions; "+
				"leave the series replica labels out of the partition hash (with block splitting: --compact.block-split.ignore-labels)", p.Partition.Index, p.Partition.Count, name)
		}
	}
	return nil
}

// PopulateBlock implements tsdb.BlockPopulator.
func (p DeduplicatingBlockPopulator) PopulateBlock(ctx context.Context, metrics *tsdb.CompactorMetrics, logger *slog.Logger, chunkPool chunkenc.Pool, mergeFunc storage.VerticalChunkSeriesMergeFunc, blocks []tsdb.BlockReader, meta *tsdb.BlockMeta, indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter, postingsFunc tsdb.IndexReaderPostingsFunc) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}
	if err := p.validate(); err != nil {
		return err
	}
	// Ties between replicas go to the source that comes first. Planning sorts
	// sources by MinTime only, and sources with the same MinTime - the copies
	// of one range that different receivers uploaded - come in no fixed
	// order, so the order is made total here: by MinTime, then by ULID.
	blocks = slices.Clone(blocks)
	slices.SortStableFunc(blocks, func(a, b tsdb.BlockReader) int {
		return cmp.Or(cmp.Compare(a.Meta().MinTime, b.Meta().MinTime), a.Meta().ULID.Compare(b.Meta().ULID))
	})

	var (
		sets        []storage.ChunkSeriesSet
		symbols     = map[string]struct{}{}
		closers     []io.Closer
		overlapping bool
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		err = errs.Err()
		metrics.PopulatingBlocks.Set(0)
	}()
	metrics.PopulatingBlocks.Set(1)

	globalMaxt := blocks[0].Meta().MaxTime
	for i, b := range blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !overlapping {
			if i > 0 && b.Meta().MinTime < globalMaxt {
				metrics.OverlappingBlocks.Inc()
				overlapping = true
				logger.Info("found overlapping blocks during compaction", "block", meta.ULID)
			}
			if b.Meta().MaxTime > globalMaxt {
				globalMaxt = b.Meta().MaxTime
			}
		}

		indexr, ierr := b.Index()
		if ierr != nil {
			return errors.Wrapf(ierr, "open index reader for block %+v", b.Meta())
		}
		closers = append(closers, indexr)
		chunkr, cerr := b.Chunks()
		if cerr != nil {
			return errors.Wrapf(cerr, "open chunk reader for block %+v", b.Meta())
		}
		closers = append(closers, chunkr)
		tombsr, terr := b.Tombstones()
		if terr != nil {
			return errors.Wrapf(terr, "open tombstone reader for block %+v", b.Meta())
		}
		closers = append(closers, tombsr)

		replicas, rerr := p.replicaPostings(ctx, postingsFunc(ctx, indexr), indexr, symbols)
		if rerr != nil {
			return errors.Wrapf(rerr, "group postings of block %+v by replica", b.Meta())
		}
		for _, refs := range replicas {
			// Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact meta.MaxTime timestamp.
			set := tsdb.NewBlockChunkSeriesSet(b.Meta().ULID, indexr, chunkr, tombsr, index.NewListPostings(refs), meta.MinTime, meta.MaxTime-1, false)
			sets = append(sets, &strippedChunkSeriesSet{set: set, without: p.ReplicaLabels})
		}
	}

	for _, s := range slices.Sorted(maps.Keys(symbols)) {
		if err := indexw.AddSymbol(s); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}

	var (
		ref      = storage.SeriesRef(0)
		chks     []chunks.Meta
		chksIter chunks.Iterator
		prev     labels.Labels
		first    = true
	)
	set := newOrderedMergeChunkSeriesSet(sets, mergeFunc)
	for set.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s := set.At()
		lset := s.Labels()
		// The merge yields strictly increasing labels by construction, and
		// the index writer would refuse anything else; say what went wrong
		// in terms of deduplication rather than of the index.
		if !first && labels.Compare(prev, lset) >= 0 {
			return errors.Errorf("deduplicated series %s does not follow %s; the replica grouping is broken", lset, prev)
		}
		first, prev = false, lset

		chksIter = s.Iterator(chksIter)
		chks = chks[:0]
		for chksIter.Next() {
			chks = append(chks, chksIter.At())
		}
		if err := chksIter.Err(); err != nil {
			return errors.Wrap(err, "chunk iter")
		}
		if len(chks) == 0 {
			continue
		}
		if err := chunkw.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := indexw.AddSeries(ref, lset, chks...); err != nil {
			return errors.Wrap(err, "add series")
		}
		if p.Stats != nil {
			p.Stats.Output++
		}

		meta.Stats.NumChunks += uint64(len(chks))
		meta.Stats.NumSeries++
		for _, chk := range chks {
			samples := uint64(chk.Chunk.NumSamples())
			meta.Stats.NumSamples += samples
			switch chk.Chunk.Encoding() {
			case chunkenc.EncHistogram, chunkenc.EncFloatHistogram:
				meta.Stats.NumHistogramSamples += samples
			case chunkenc.EncXOR:
				meta.Stats.NumFloatSamples += samples
			}
		}
		for _, chk := range chks {
			if err := chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}
		ref++
	}
	if err := set.Err(); err != nil {
		return errors.Wrap(err, "iterate compaction set")
	}
	return nil
}

// replicaPostings walks one block's postings and returns, per value of the
// replica labels in ascending order, the series of that replica ordered by
// their labels without the replica labels, collecting the symbols the
// stripped labels use.
//
// Dropping a label does not keep the index order - {a="1", r="x"} sorts
// before {a="1", b="1", r="x"}, but {a="1"} after {a="1", b="1"} - except
// among series with the same label names and the same replica values: there
// the dropped labels sit at the same position with the same value in every
// comparison and never decide one. The postings are therefore grouped by
// replica value and label names, each group is already in stripped order,
// and the groups of one replica are merged with a heap.
func (p DeduplicatingBlockPopulator) replicaPostings(ctx context.Context, all index.Postings, indexr tsdb.IndexReader, symbols map[string]struct{}) ([][]storage.SeriesRef, error) {
	type groupKey struct{ replica, names string }
	var (
		groups   = map[groupKey][]storage.SeriesRef{}
		replicas = map[string]labels.Labels{}
		builder  labels.ScratchBuilder
		digest   = xxhash.New()
		n        int
		key      strings.Builder
	)
	for all.Next() {
		n++
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, errors.Wrap(err, "walk postings")
			}
		}
		if err := indexr.Series(all.At(), &builder, nil); err != nil {
			return nil, errors.Wrap(err, "read series")
		}
		if p.PartitionStats != nil {
			p.PartitionStats.Walked++
		}
		lset := builder.Labels()
		if p.Partition != nil && !p.Partition.contains(lset, digest) {
			continue
		}
		if p.PartitionStats != nil {
			p.PartitionStats.Kept++
		}
		if p.Stats != nil {
			p.Stats.Input++
		}

		key.Reset()
		for _, name := range p.ReplicaLabels {
			if v := lset.Get(name); v != "" {
				key.WriteString(name)
				key.WriteByte('\xff')
				key.WriteString(v)
				key.WriteByte('\xff')
			}
		}
		replica := key.String()
		if _, ok := replicas[replica]; !ok {
			b := labels.NewScratchBuilder(len(p.ReplicaLabels))
			for _, name := range p.ReplicaLabels {
				if v := lset.Get(name); v != "" {
					b.Add(strings.Clone(name), strings.Clone(v))
				}
			}
			replicas[replica] = b.Labels()
		}
		key.Reset()
		lset.Range(func(l labels.Label) {
			if slices.Contains(p.ReplicaLabels, l.Name) {
				return
			}
			key.WriteString(l.Name)
			key.WriteByte('\xff')
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		})
		k := groupKey{replica: replica, names: key.String()}
		groups[k] = append(groups[k], all.At())
	}
	if err := all.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate postings")
	}

	byReplica := map[string][][]storage.SeriesRef{}
	for k, refs := range groups {
		byReplica[k.replica] = append(byReplica[k.replica], refs)
	}
	order := slices.SortedFunc(maps.Keys(byReplica), func(a, b string) int { return labels.Compare(replicas[a], replicas[b]) })

	out := make([][]storage.SeriesRef, 0, len(order))
	for _, r := range order {
		merged, err := p.mergeGroups(byReplica[r], indexr)
		if err != nil {
			return nil, errors.Wrap(err, "merge postings groups")
		}
		out = append(out, merged)
	}
	return out, nil
}

// mergeGroups merges postings lists that are each in stripped label order
// into one, in stripped label order.
func (p DeduplicatingBlockPopulator) mergeGroups(lists [][]storage.SeriesRef, indexr tsdb.IndexReader) ([]storage.SeriesRef, error) {
	if len(lists) == 1 {
		return lists[0], nil
	}
	total := 0
	for _, l := range lists {
		total += len(l)
	}
	var builder labels.ScratchBuilder
	stripped := func(ref storage.SeriesRef) (labels.Labels, error) {
		if err := indexr.Series(ref, &builder, nil); err != nil {
			return labels.EmptyLabels(), errors.Wrap(err, "read series")
		}
		return labels.NewBuilder(builder.Labels()).Del(p.ReplicaLabels...).Labels(), nil
	}
	h := make(refCursorHeap, 0, len(lists))
	for _, l := range lists {
		lset, err := stripped(l[0])
		if err != nil {
			return nil, errors.Wrap(err, "read first series of postings group")
		}
		h = append(h, &refCursor{refs: l, lset: lset})
	}
	heap.Init(&h)
	out := make([]storage.SeriesRef, 0, total)
	for len(h) > 0 {
		c := h[0]
		out = append(out, c.refs[c.pos])
		c.pos++
		if c.pos == len(c.refs) {
			heap.Pop(&h)
			continue
		}
		lset, err := stripped(c.refs[c.pos])
		if err != nil {
			return nil, errors.Wrap(err, "read next series of postings group")
		}
		if labels.Compare(c.lset, lset) >= 0 {
			return nil, errors.Errorf("postings of series %s are not in label order", lset)
		}
		c.lset = lset
		heap.Fix(&h, 0)
	}
	return out, nil
}

type refCursor struct {
	refs []storage.SeriesRef
	pos  int
	lset labels.Labels
}

type refCursorHeap []*refCursor

func (h refCursorHeap) Len() int           { return len(h) }
func (h refCursorHeap) Less(i, j int) bool { return labels.Compare(h[i].lset, h[j].lset) < 0 }
func (h refCursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *refCursorHeap) Push(x any)        { *h = append(*h, x.(*refCursor)) }
func (h *refCursorHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// strippedChunkSeriesSet presents the series of a set without some labels.
// The set's order must already be the order of the stripped labels.
type strippedChunkSeriesSet struct {
	set     storage.ChunkSeriesSet
	without []string
	cur     storage.ChunkSeries
}

func (s *strippedChunkSeriesSet) Next() bool {
	if !s.set.Next() {
		s.cur = nil
		return false
	}
	at := s.set.At()
	s.cur = strippedChunkSeries{ChunkSeries: at, lset: labels.NewBuilder(at.Labels()).Del(s.without...).Labels()}
	return true
}

func (s *strippedChunkSeriesSet) At() storage.ChunkSeries           { return s.cur }
func (s *strippedChunkSeriesSet) Err() error                        { return s.set.Err() }
func (s *strippedChunkSeriesSet) Warnings() annotations.Annotations { return s.set.Warnings() }

type strippedChunkSeries struct {
	storage.ChunkSeries
	lset labels.Labels
}

func (s strippedChunkSeries) Labels() labels.Labels { return s.lset }

// orderedMergeChunkSeriesSet merges sets each in label order into one,
// merging series with equal labels with mergeFunc. Unlike
// storage.NewMergeChunkSeriesSet it offers equal series to mergeFunc in the
// order of their sets, so that a merge function whose result depends on the
// order - one that breaks ties by position, as penalty deduplication does -
// gives a result that does not depend on how a heap happened to settle.
type orderedMergeChunkSeriesSet struct {
	sets      []storage.ChunkSeriesSet
	mergeFunc storage.VerticalChunkSeriesMergeFunc
	h         setCursorHeap
	current   []int
	started   bool
	cur       storage.ChunkSeries
	err       error
}

func newOrderedMergeChunkSeriesSet(sets []storage.ChunkSeriesSet, mergeFunc storage.VerticalChunkSeriesMergeFunc) *orderedMergeChunkSeriesSet {
	return &orderedMergeChunkSeriesSet{sets: sets, mergeFunc: mergeFunc}
}

func (m *orderedMergeChunkSeriesSet) Next() bool {
	if m.err != nil {
		return false
	}
	if m.started {
		// The sets whose series make up the current one advance only now:
		// the caller was done with it when it asked for the next.
		for _, i := range m.current {
			s := m.sets[i]
			if s.Next() {
				heap.Push(&m.h, setCursor{index: i, set: s})
				continue
			}
			if err := s.Err(); err != nil {
				m.err = err
				return false
			}
		}
	}
	if !m.started {
		m.started = true
		for i, s := range m.sets {
			if s.Next() {
				m.h = append(m.h, setCursor{index: i, set: s})
				continue
			}
			if err := s.Err(); err != nil {
				m.err = err
				return false
			}
		}
		heap.Init(&m.h)
	}
	m.current = m.current[:0]
	if len(m.h) == 0 {
		m.cur = nil
		return false
	}
	// Popping equal labels comes out in set order: ties are broken by index.
	first := heap.Pop(&m.h).(setCursor)
	m.current = append(m.current, first.index)
	series := []storage.ChunkSeries{first.set.At()}
	for len(m.h) > 0 && labels.Equal(m.h[0].set.At().Labels(), series[0].Labels()) {
		c := heap.Pop(&m.h).(setCursor)
		m.current = append(m.current, c.index)
		series = append(series, c.set.At())
	}
	m.cur = series[0]
	if len(series) > 1 {
		m.cur = m.mergeFunc(series...)
	}
	return true
}

func (m *orderedMergeChunkSeriesSet) At() storage.ChunkSeries { return m.cur }
func (m *orderedMergeChunkSeriesSet) Err() error              { return m.err }
func (m *orderedMergeChunkSeriesSet) Warnings() annotations.Annotations {
	var ws annotations.Annotations
	for _, s := range m.sets {
		ws.Merge(s.Warnings())
	}
	return ws
}

type setCursor struct {
	index int
	set   storage.ChunkSeriesSet
}

type setCursorHeap []setCursor

func (h setCursorHeap) Len() int { return len(h) }
func (h setCursorHeap) Less(i, j int) bool {
	if c := labels.Compare(h[i].set.At().Labels(), h[j].set.At().Labels()); c != 0 {
		return c < 0
	}
	return h[i].index < h[j].index
}
func (h setCursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *setCursorHeap) Push(x any)   { *h = append(*h, x.(setCursor)) }
func (h *setCursorHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

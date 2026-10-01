// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"bytes"
	"container/heap"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// NewChunkSeriesMerger merges several chunk series into one.
// Deduplication is based on penalty based deduplication algorithm without handling counter reset.
//
// A chunk that overlaps no other chunk is passed through as it is. Chunks that
// overlap, directly or through a chain of overlaps, are deduplicated together:
// the chunks each input series has among them are read as one series, those
// series are deduplicated with the penalty algorithm the querier uses,
// preferring them in the order the input series are given, and the result is
// encoded into new chunks.
func NewChunkSeriesMerger() storage.VerticalChunkSeriesMergeFunc {
	return func(series ...storage.ChunkSeries) storage.ChunkSeries {
		if len(series) == 0 {
			return nil
		}
		return &storage.ChunkSeriesEntry{
			Lset: series[0].Labels(),
			ChunkIteratorFn: func(iterator chunks.Iterator) chunks.Iterator {
				iterators := make([]chunks.Iterator, 0, len(series))
				for _, s := range series {
					iterators = append(iterators, s.Iterator(nil))
				}
				return &dedupChunksIterator{
					iterators: iterators,
				}
			},
		}
	}
}

type dedupChunksIterator struct {
	iterators []chunks.Iterator
	h         chunkIteratorHeap

	// group is the current group of overlapping chunks.
	group []groupChunk
	// merged holds the chunks deduplicated from the current group that are
	// yet to be returned.
	merged chunks.Iterator

	err  error
	curr chunks.Meta
}

// groupChunk is a chunk of a group of overlapping chunks, along with the
// index of the input series it comes from.
type groupChunk struct {
	chunks.Meta
	input int
}

func (d *dedupChunksIterator) At() chunks.Meta {
	return d.curr
}

// Next method is based on https://github.com/prometheus/prometheus/blob/v2.27.1/storage/merge.go#L615.
// The difference is that it handles both XOR/Histogram/FloatHistogram and Aggr chunk Encoding.
func (d *dedupChunksIterator) Next() bool {
	if d.h == nil {
		d.h = make(chunkIteratorHeap, 0, len(d.iterators))
		for i, iter := range d.iterators {
			if iter.Next() {
				heap.Push(&d.h, &inputChunkIterator{Iterator: iter, input: i})
			}
		}
	}
	for {
		if d.merged != nil {
			if d.merged.Next() {
				d.curr = d.merged.At()
				return true
			}
			if d.err = d.merged.Err(); d.err != nil {
				return false
			}
			d.merged = nil
		}
		if len(d.h) == 0 {
			return false
		}

		d.nextGroup()
		if len(d.group) == 1 {
			// No overlap, the chunk is passed through as it is.
			d.curr = d.group[0].Meta
			return true
		}
		d.merged = mergeOverlappingChunks(d.group, len(d.iterators))
	}
}

// nextGroup collects the oldest chunk left and every chunk that overlaps it,
// directly or through a chain of overlaps, in the order of their min time.
func (d *dedupChunksIterator) nextGroup() {
	first := d.popChunk()
	d.group = append(d.group[:0], first)

	maxTime, prev := first.MaxTime, first.Meta
	for len(d.h) > 0 {
		// Get the next oldest chunk by min, then max time.
		if d.h[0].At().MinTime > maxTime {
			// No overlap with the group.
			break
		}
		next := d.popChunk()
		if next.MinTime == prev.MinTime &&
			next.MaxTime == prev.MaxTime &&
			bytes.Equal(next.Chunk.Bytes(), prev.Chunk.Bytes()) {
			// 1:1 duplicates, skip it.
			continue
		}
		d.group = append(d.group, next)
		maxTime = max(maxTime, next.MaxTime)
		prev = next.Meta
	}
}

// popChunk returns the oldest chunk left and advances the iterator it comes from.
func (d *dedupChunksIterator) popChunk() groupChunk {
	iter := d.h[0]
	chk := groupChunk{Meta: iter.At(), input: iter.input}
	if iter.Next() {
		heap.Fix(&d.h, 0)
	} else {
		heap.Pop(&d.h)
	}
	return chk
}

func (d *dedupChunksIterator) Err() error {
	if d.err != nil {
		return d.err
	}
	for _, iter := range d.iterators {
		if err := iter.Err(); err != nil {
			return err
		}
	}
	return nil
}

// inputChunkIterator iterates over the chunks of one input series.
type inputChunkIterator struct {
	chunks.Iterator
	input int
}

type chunkIteratorHeap []*inputChunkIterator

func (h chunkIteratorHeap) Len() int      { return len(h) }
func (h chunkIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h chunkIteratorHeap) Less(i, j int) bool {
	at := h[i].At()
	bt := h[j].At()
	if at.MinTime == bt.MinTime {
		if at.MaxTime == bt.MaxTime {
			return h[i].input < h[j].input
		}
		return at.MaxTime < bt.MaxTime
	}
	return at.MinTime < bt.MinTime
}

func (h *chunkIteratorHeap) Push(x any) {
	*h = append(*h, x.(*inputChunkIterator))
}

func (h *chunkIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// mergeOverlappingChunks deduplicates a group of overlapping chunks of the
// given number of input series. The chunks of each input series are read as
// one series, and those series are deduplicated with the penalty algorithm,
// in the order of the input series, the same way the querier deduplicates
// replicas (see dedupSeries).
//
// Merging the chunks pairwise instead would treat the next chunk of the same
// input series as another replica: when the first chunk ends, the penalty the
// next one got as a replica would skip samples that both input series have.
func mergeOverlappingChunks(group []groupChunk, numInputs int) chunks.Iterator {
	// Raw and downsampled chunks are never compacted together; the oldest
	// chunk decides which ones are deduplicated.
	isAggr := group[0].Chunk.Encoding() == downsample.ChunkEncAggr

	perInput := make([][]chunks.Meta, numInputs)
	for _, chk := range group {
		switch chk.Chunk.Encoding() {
		case chunkenc.EncXOR, chunkenc.EncHistogram, chunkenc.EncFloatHistogram:
			if isAggr {
				continue
			}
		case downsample.ChunkEncAggr:
			if !isAggr {
				continue
			}
		default:
			continue
		}
		perInput[chk.input] = append(perInput[chk.input], chk.Meta)
	}

	if isAggr {
		return mergeAggrChunks(perInput)
	}
	return storage.NewSeriesToChunkEncoder(&storage.SeriesEntry{
		SampleIteratorFn: func(chunkenc.Iterator) chunkenc.Iterator {
			replicas := make([]adjustableSeriesIterator, 0, len(perInput))
			for _, metas := range perInput {
				if len(metas) == 0 {
					continue
				}
				chks := make([]chunkenc.Chunk, 0, len(metas))
				for _, m := range metas {
					chks = append(chks, m.Chunk)
				}
				replicas = append(replicas, newReplicaIterator(chks, overlapping(metas)))
			}
			if len(replicas) == 0 {
				return chunkenc.NewNopIterator()
			}
			return newPenaltyDedupIterator(replicas)
		},
	}).Iterator(nil)
}

// mergeAggrChunks deduplicates each aggregate of the downsampled chunks of a
// group on its own, and encodes the results into aggregated chunks.
func mergeAggrChunks(perInput [][]chunks.Meta) chunks.Iterator {
	var samplesIter [5]chunkenc.Iterator
	for at := downsample.AggrCount; at <= downsample.AggrCounter; at++ {
		var replicas []adjustableSeriesIterator
		for _, metas := range perInput {
			var chks []chunkenc.Chunk
			for _, m := range metas {
				aggrChk, ok := m.Chunk.(*downsample.AggrChunk)
				if !ok {
					continue
				}
				if c, err := aggrChk.Get(at); err == nil {
					chks = append(chks, c)
				}
			}
			if len(chks) > 0 {
				replicas = append(replicas, newReplicaIterator(chks, overlapping(metas)))
			}
		}
		if len(replicas) > 0 {
			samplesIter[at] = newPenaltyDedupIterator(replicas)
		}
	}
	if samplesIter[downsample.AggrCount] == nil {
		return errChunksIterator{err: errors.New("deduplicate downsampled chunks: no count aggregate")}
	}
	return newAggrChunkIterator(samplesIter)
}

// overlapping reports whether any of the chunks, sorted by min time, overlap.
func overlapping(metas []chunks.Meta) bool {
	for i := 1; i < len(metas); i++ {
		// As long as no chunks overlap, the previous one ends last.
		if metas[i].MinTime <= metas[i-1].MaxTime {
			return true
		}
	}
	return false
}

// newReplicaIterator returns an iterator over the samples of the chunks one
// input series has in a group, sorted by min time. They are normally
// consecutive and are read one after another; overlapping ones are merged
// by timestamp, keeping one sample per timestamp.
func newReplicaIterator(chks []chunkenc.Chunk, overlapping bool) adjustableSeriesIterator {
	if len(chks) == 1 {
		return noopAdjustableSeriesIterator{chks[0].Iterator(nil)}
	}
	if overlapping {
		iters := make([]chunkenc.Iterator, 0, len(chks))
		for _, c := range chks {
			iters = append(iters, c.Iterator(nil))
		}
		return noopAdjustableSeriesIterator{storage.ChainSampleIteratorFromIterators(nil, iters)}
	}
	return noopAdjustableSeriesIterator{&consecutiveChunksIterator{chks: chks, curr: chks[0].Iterator(nil)}}
}

// consecutiveChunksIterator iterates over the samples of chunks that follow
// each other in time without overlapping.
type consecutiveChunksIterator struct {
	chks []chunkenc.Chunk
	i    int
	curr chunkenc.Iterator
}

func (it *consecutiveChunksIterator) Next() chunkenc.ValueType {
	for {
		if vt := it.curr.Next(); vt != chunkenc.ValNone || !it.nextChunk() {
			return vt
		}
	}
}

func (it *consecutiveChunksIterator) Seek(t int64) chunkenc.ValueType {
	for {
		if vt := it.curr.Seek(t); vt != chunkenc.ValNone || !it.nextChunk() {
			return vt
		}
	}
}

// nextChunk moves on to the next chunk, unless the current one failed or is the last one.
func (it *consecutiveChunksIterator) nextChunk() bool {
	if it.curr.Err() != nil || it.i+1 >= len(it.chks) {
		return false
	}
	it.i++
	it.curr = it.chks[it.i].Iterator(nil)
	return true
}

func (it *consecutiveChunksIterator) At() (int64, float64) {
	return it.curr.At()
}

func (it *consecutiveChunksIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return it.curr.AtHistogram(h)
}

func (it *consecutiveChunksIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.curr.AtFloatHistogram(fh)
}

func (it *consecutiveChunksIterator) AtT() int64 {
	return it.curr.AtT()
}

func (it *consecutiveChunksIterator) Err() error {
	return it.curr.Err()
}

type errChunksIterator struct {
	err error
}

func (errChunksIterator) At() chunks.Meta { return chunks.Meta{} }
func (errChunksIterator) Next() bool      { return false }
func (e errChunksIterator) Err() error    { return e.err }

type aggrChunkIterator struct {
	iters [5]chunkenc.Iterator
	// pending holds the value type of the sample each aggregate's iterator
	// read past the end of the previous chunk, or ValNone. That sample is yet
	// to be encoded.
	pending      [5]chunkenc.ValueType
	curr         chunks.Meta
	countChkIter chunks.Iterator

	err error
}

func newAggrChunkIterator(iters [5]chunkenc.Iterator) chunks.Iterator {
	return &aggrChunkIterator{
		iters: iters,
		countChkIter: storage.NewSeriesToChunkEncoder(&storage.SeriesEntry{
			SampleIteratorFn: func(_ chunkenc.Iterator) chunkenc.Iterator {
				return iters[downsample.AggrCount]
			},
		}).Iterator(nil),
	}
}

func (a *aggrChunkIterator) Next() bool {
	if !a.countChkIter.Next() {
		if err := a.countChkIter.Err(); err != nil {
			a.err = err
		}
		return false
	}

	countChk := a.countChkIter.At()
	mint := countChk.MinTime
	maxt := countChk.MaxTime

	var (
		chks [5]chunkenc.Chunk
		chk  *chunks.Meta
		err  error
	)

	chks[downsample.AggrCount] = countChk.Chunk
	for i := downsample.AggrSum; i <= downsample.AggrCounter; i++ {
		chk, err = a.toChunk(i, mint, maxt)
		if err != nil {
			a.err = err
			return false
		}
		if chk != nil {
			chks[i] = chk.Chunk
		}
	}

	a.curr = chunks.Meta{
		MinTime: mint,
		MaxTime: maxt,
		Chunk:   downsample.EncodeAggrChunk(chks),
	}
	return true
}

func (a *aggrChunkIterator) At() chunks.Meta {
	return a.curr
}

func (a *aggrChunkIterator) Err() error {
	return a.err
}

func (a *aggrChunkIterator) toChunk(at downsample.AggrType, minTime, maxTime int64) (*chunks.Meta, error) {
	if a.iters[at] == nil {
		return nil, nil
	}
	c := chunkenc.NewXORChunk()
	appender, err := c.Appender()
	if err != nil {
		return nil, err
	}

	it := a.iters[at]

	var (
		lastT int64
		lastV float64
	)
	// Reading the previous chunk stopped at the first sample after it, which
	// can be the first sample of this chunk.
	vt := a.pending[at]
	a.pending[at] = chunkenc.ValNone
	if vt == chunkenc.ValNone {
		vt = it.Next()
	}
	for ; vt != chunkenc.ValNone; vt = it.Next() {
		t, v := it.At()
		if t < minTime {
			continue
		}
		if t > maxTime {
			a.pending[at] = vt
			break
		}
		lastT, lastV = t, v
		appender.Append(lastT, lastV)
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	// No sample in the required time range.
	if lastT == 0 && lastV == 0 {
		return nil, nil
	}

	// Encode last sample for AggrCounter.
	if at == downsample.AggrCounter {
		appender.Append(lastT, lastV)
	}

	return &chunks.Meta{
		MinTime: minTime,
		MaxTime: maxTime,
		Chunk:   c,
	}, nil
}

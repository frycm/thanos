// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"slices"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// SeriesShard names one shard of a series set split by hash (metadata.SplitHashStable): the series whose hash
// modulo Count is Index-1. Index is 1-based, like the value of metadata.CompactorShardIDLabel.
//
// The hash is labels.StableHash of the series labels less the labels named in IgnoreLabels, so that series that
// differ only in those (replicas that carry their replica label inside the series, say) fall into the same shard and
// can be deduplicated there. A series that carries none of them hashes exactly as labels.StableHash hashes it.
type SeriesShard struct {
	Index        int
	Count        int
	IgnoreLabels []string
}

// SeriesShardFor returns the index-th shard of the given scheme.
func SeriesShardFor(scheme metadata.SplitScheme, index int) SeriesShard {
	return SeriesShard{Index: index, Count: scheme.Shards, IgnoreLabels: scheme.IgnoreLabels}
}

// Validate checks that the shard is well-formed.
func (p SeriesShard) Validate() error {
	if p.Count < 1 || p.Index < 1 || p.Index > p.Count {
		return errors.Errorf("invalid series shard %d of %d", p.Index, p.Count)
	}
	if slices.Contains(p.IgnoreLabels, "") {
		return errors.Errorf("series shard %d of %d ignores a label with no name", p.Index, p.Count)
	}
	return nil
}

// Contains reports whether the series belongs to the shard.
func (p SeriesShard) Contains(lset labels.Labels) bool {
	return p.contains(lset, nil)
}

// contains is Contains with a digest to reuse across calls; nil allocates one when needed.
func (p SeriesShard) contains(lset labels.Labels, h *xxhash.Digest) bool {
	return int(p.hash(lset, h)%uint64(p.Count))+1 == p.Index
}

// sep separates names and values in the byte layout labels.StableHash hashes, which hash mirrors.
const sep = "\xff"

// hash is labels.StableHash of lset without the labels in IgnoreLabels: the same byte layout, name and value each
// followed by a separator, fed to xxhash for every label kept. A series that carries none of the labels hashes
// exactly as labels.StableHash would.
func (p SeriesShard) hash(lset labels.Labels, h *xxhash.Digest) uint64 {
	if len(p.IgnoreLabels) == 0 {
		return labels.StableHash(lset)
	}
	if h == nil {
		h = xxhash.New()
	}
	h.Reset()
	lset.Range(func(l labels.Label) {
		if slices.Contains(p.IgnoreLabels, l.Name) {
			return
		}
		_, _ = h.WriteString(l.Name)
		_, _ = h.WriteString(sep)
		_, _ = h.WriteString(l.Value)
		_, _ = h.WriteString(sep)
	})
	return h.Sum64()
}

// PartitionedBlockPopulator is a tsdb.BlockPopulator that writes one shard of a compaction: only the series of the
// shard, and only the symbols those series use. Everything else - merging, deduplication, tombstones, chunk writing -
// is the default populator's logic. When the shard holds no series, the compactor writes no block; the compactor
// wrapper returned by SplitGrouper.Compactor then writes an empty block, so that every shard of a split exists.
type PartitionedBlockPopulator struct {
	Shard SeriesShard
	// Stats, if set, receives the populator's tally of the sources' series.
	Stats *PartitionStats
}

// PartitionStats is the tally a PartitionedBlockPopulator keeps of the series it walked in the sources and the series
// among them it kept for its shard, both counted once per source block that holds the series. The populators of every
// shard of one compaction walk the same series, so whoever runs them can tell from their tallies whether the shards
// together kept every series.
type PartitionStats struct {
	Walked uint64
	Kept   uint64
}

var _ tsdb.BlockPopulator = PartitionedBlockPopulator{}

func (PartitionedBlockPopulator) keepsEmptyBlock() {}

// PopulateBlock implements tsdb.BlockPopulator. It mirrors tsdb.DefaultBlockPopulator.PopulateBlock, except that each
// block's postings are filtered to the shard before merging and the symbol table is built from the kept series rather
// than merged from the source blocks' tables.
func (p PartitionedBlockPopulator) PopulateBlock(
	ctx context.Context,
	metrics *tsdb.CompactorMetrics,
	logger *slog.Logger,
	chunkPool chunkenc.Pool,
	mergeFunc storage.VerticalChunkSeriesMergeFunc,
	blocks []tsdb.BlockReader,
	meta *tsdb.BlockMeta,
	indexw tsdb.IndexWriter,
	chunkw tsdb.ChunkWriter,
	postingsFunc tsdb.IndexReaderPostingsFunc,
) (rerr error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}
	if err := p.Shard.Validate(); err != nil {
		return err
	}

	var (
		sets        []storage.ChunkSeriesSet
		symbols     = map[string]struct{}{}
		closers     []io.Closer
		overlapping bool
	)
	defer func() {
		errs := tsdb_errors.NewMulti(rerr)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		rerr = errs.Err()
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

		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %+v", b.Meta())
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %+v", b.Meta())
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %+v", b.Meta())
		}
		closers = append(closers, tombsr)

		refs, err := p.partitionPostings(ctx, postingsFunc(ctx, indexr), indexr, symbols)
		if err != nil {
			return errors.Wrapf(err, "partition postings of block %+v", b.Meta())
		}
		// Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact meta.MaxTime timestamp.
		sets = append(sets, tsdb.NewBlockChunkSeriesSet(b.Meta().ULID, indexr, chunkr, tombsr, index.NewListPostings(refs), meta.MinTime, meta.MaxTime-1, false))
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
	)

	set := sets[0]
	if len(sets) > 1 {
		set = storage.NewMergeChunkSeriesSet(sets, 0, mergeFunc)
	}

	for set.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s := set.At()
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
		if err := indexw.AddSeries(ref, s.Labels(), chks...); err != nil {
			return errors.Wrap(err, "add series")
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

// partitionPostings walks the postings and keeps the series of the shard, collecting the symbols they use.
func (p PartitionedBlockPopulator) partitionPostings(ctx context.Context, all index.Postings, indexr tsdb.IndexReader, symbols map[string]struct{}) ([]storage.SeriesRef, error) {
	var (
		refs    []storage.SeriesRef
		builder labels.ScratchBuilder
		digest  = xxhash.New()
		n       int
	)
	for all.Next() {
		n++
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if err := indexr.Series(all.At(), &builder, nil); err != nil {
			return nil, err
		}
		if p.Stats != nil {
			p.Stats.Walked++
		}
		lset := builder.Labels()
		if !p.Shard.contains(lset, digest) {
			continue
		}
		if p.Stats != nil {
			p.Stats.Kept++
		}
		refs = append(refs, all.At())
		lset.Range(func(l labels.Label) {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		})
	}
	return refs, all.Err()
}

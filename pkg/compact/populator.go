// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
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
)

// PartitionedBlockPopulator is a tsdb.BlockPopulator that writes one partition
// of a compaction: only the series of the partition, and only the symbols
// those series use. Everything else - merging, deduplication, tombstones,
// chunk writing - is the default populator's logic.
type PartitionedBlockPopulator struct {
	Partition SeriesPartition
	// Stats, if set, receives the populator's tally of the sources' series.
	Stats *PartitionStats
}

// PartitionStats is the tally a PartitionedBlockPopulator keeps of the
// series it walked in the sources and the series among them it kept for its
// partition, both counted once per source block that holds the series. The
// populators of every partition of one compaction walk the same series, so
// whoever runs them can tell from their tallies whether the partitions
// together kept every series.
type PartitionStats struct {
	Walked uint64
	Kept   uint64
}

var _ tsdb.BlockPopulator = PartitionedBlockPopulator{}

// PopulateBlock implements tsdb.BlockPopulator. It mirrors
// tsdb.DefaultBlockPopulator.PopulateBlock, except that each block's postings
// are filtered to the partition before merging and the symbol table is built
// from the kept series rather than merged from the source blocks' tables.
func (p PartitionedBlockPopulator) PopulateBlock(ctx context.Context, metrics *tsdb.CompactorMetrics, logger *slog.Logger, chunkPool chunkenc.Pool, mergeFunc storage.VerticalChunkSeriesMergeFunc, blocks []tsdb.BlockReader, meta *tsdb.BlockMeta, indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter, postingsFunc tsdb.IndexReaderPostingsFunc) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}
	if err := p.Partition.Validate(); err != nil {
		return err
	}

	var (
		sets        []storage.ChunkSeriesSet
		symbols     = map[string]struct{}{}
		closers     []io.Closer
		overlapping bool
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(fmt.Errorf("close: %w", cerr))
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
				logger.Info("Found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.Meta().MaxTime > globalMaxt {
				globalMaxt = b.Meta().MaxTime
			}
		}

		indexr, err := b.Index()
		if err != nil {
			return fmt.Errorf("open index reader for block %+v: %w", b.Meta(), err)
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return fmt.Errorf("open chunk reader for block %+v: %w", b.Meta(), err)
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return fmt.Errorf("open tombstone reader for block %+v: %w", b.Meta(), err)
		}
		closers = append(closers, tombsr)

		refs, err := p.partitionPostings(ctx, postingsFunc(ctx, indexr), indexr, symbols)
		if err != nil {
			return fmt.Errorf("partition postings of block %+v: %w", b.Meta(), err)
		}
		// Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact meta.MaxTime timestamp.
		sets = append(sets, tsdb.NewBlockChunkSeriesSet(b.Meta().ULID, indexr, chunkr, tombsr, index.NewListPostings(refs), meta.MinTime, meta.MaxTime-1, false))
	}

	for _, s := range slices.Sorted(maps.Keys(symbols)) {
		if err := indexw.AddSymbol(s); err != nil {
			return fmt.Errorf("add symbol: %w", err)
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
			return fmt.Errorf("chunk iter: %w", err)
		}
		if len(chks) == 0 {
			continue
		}
		if err := chunkw.WriteChunks(chks...); err != nil {
			return fmt.Errorf("write chunks: %w", err)
		}
		if err := indexw.AddSeries(ref, s.Labels(), chks...); err != nil {
			return fmt.Errorf("add series: %w", err)
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
				return fmt.Errorf("put chunk: %w", err)
			}
		}
		ref++
	}
	if err := set.Err(); err != nil {
		return fmt.Errorf("iterate compaction set: %w", err)
	}
	return nil
}

// partitionPostings walks the postings and keeps the series of the partition,
// collecting the symbols they use.
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
		if !p.Partition.contains(lset, digest) {
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

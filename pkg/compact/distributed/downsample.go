// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// DownsampleTask builds the task that asks a worker to downsample one block.
//
// The candidate metadata comes from the manager's synced view, where the
// deduplication replica labels are already removed; the labels are carried so
// the worker can strip the raw metadata it fetches from the bucket the same
// way, exactly as the compaction path does.
func DownsampleTask(m *metadata.Meta, targetResolution int64, hashFunc metadata.HashFunc, blockFilesConcurrency int, acceptMalformedIndex bool, dedupReplicaLabels []string) Task {
	series, indexBytes := expectedTaskSize([]*metadata.Meta{m})
	return Task{
		ID:                 ulid.Make().String(),
		Type:               TaskDownsample,
		SourceBlocks:       []string{m.ULID.String()},
		ExpectedMinTime:    m.MinTime,
		ExpectedMaxTime:    m.MaxTime,
		TargetResolution:   targetResolution,
		ExpectedSeries:     series,
		ExpectedIndexBytes: indexBytes,
		Group: GroupSpec{
			Key:                   m.Thanos.GroupKey(),
			Labels:                m.Thanos.Labels,
			Resolution:            m.Thanos.Downsample.Resolution,
			AcceptMalformedIndex:  acceptMalformedIndex,
			HashFunc:              string(hashFunc),
			BlockFilesConcurrency: blockFilesConcurrency,
			DedupReplicaLabels:    dedupReplicaLabels,
		},
	}
}

// executeDownsample downsamples a single block and uploads the result.
//
// It mirrors what the compactor does in process, with one addition: the worker
// re-checks that it still owns the task immediately before uploading, and
// discards its work if it cannot confirm that.
func (w *Worker) executeDownsample(ctx context.Context, task Task, dir string, preUploadCheck func(context.Context) error) ([]ulid.ULID, error) {
	if len(task.SourceBlocks) != 1 {
		return nil, errors.Errorf("a downsample task needs exactly one source block, got %d", len(task.SourceBlocks))
	}
	id, err := ulid.Parse(task.SourceBlocks[0])
	if err != nil {
		return nil, errors.Wrapf(err, "parse source block ID %q", task.SourceBlocks[0])
	}

	meta, err := block.DownloadMeta(ctx, w.logger, w.bkt, id)
	if err != nil {
		return nil, compact.NewRetryError(errors.Wrapf(err, "read metadata of source block %s", id))
	}
	m := &meta
	// The downsampled block inherits this metadata's labels, and the manager
	// verifies them against its replica-stripped view - and in standalone mode
	// downsampling also runs on the stripped metadata. Strip the same way.
	stripDedupReplicaLabels(m, task.Group.DedupReplicaLabels)

	if m.MinTime != task.ExpectedMinTime || m.MaxTime != task.ExpectedMaxTime {
		return nil, errors.Errorf("source block spans [%d, %d] but the task was planned for [%d, %d]",
			m.MinTime, m.MaxTime, task.ExpectedMinTime, task.ExpectedMaxTime)
	}

	acceptMalformedIndex := task.Group.AcceptMalformedIndex
	bdir := filepath.Join(dir, m.ULID.String())

	begin := time.Now()
	if err := block.Download(ctx, w.logger, w.bkt, m.ULID, bdir, objstore.WithFetchConcurrency(task.Group.BlockFilesConcurrency)); err != nil {
		return nil, compact.NewRetryError(errors.Wrapf(err, "download block %s", m.ULID))
	}
	w.m.stageDuration.WithLabelValues("download").Observe(time.Since(begin).Seconds())

	if err := block.VerifyIndex(ctx, w.logger, filepath.Join(bdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil && !acceptMalformedIndex {
		return nil, errors.Wrap(err, "input block index not valid")
	}

	begin = time.Now()
	var pool chunkenc.Pool
	if m.Thanos.Downsample.Resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = downsample.NewPool()
	}

	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(w.logger), bdir, pool, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "open block %s", m.ULID)
	}
	defer runutil.CloseWithLogOnErr(log.With(w.logger, "outcome", "potential left mmap file handlers left"), b, "tsdb reader")

	outID, err := downsample.Downsample(ctx, w.logger, m, b, dir, task.TargetResolution)
	if err != nil {
		return nil, errors.Wrapf(err, "downsample block %s to window %d", m.ULID, task.TargetResolution)
	}
	resdir := filepath.Join(dir, outID.String())
	w.m.stageDuration.WithLabelValues("downsample").Observe(time.Since(begin).Seconds())

	level.Info(w.logger).Log("msg", "downsampled block", "from", m.ULID, "to", outID, "duration", time.Since(begin))

	stats, err := block.GatherIndexHealthStats(ctx, w.logger, filepath.Join(resdir, block.IndexFilename), m.MinTime, m.MaxTime)
	if err == nil {
		err = stats.AnyErr()
	}
	if err != nil && !acceptMalformedIndex {
		return nil, errors.Wrap(err, "output block index not valid")
	}

	outMeta, err := metadata.ReadFromDir(resdir)
	if err != nil {
		return nil, errors.Wrap(err, "read meta")
	}
	if stats.ChunkMaxSize > 0 {
		outMeta.Thanos.IndexStats.ChunkMaxSize = stats.ChunkMaxSize
	}
	if stats.SeriesMaxSize > 0 {
		outMeta.Thanos.IndexStats.SeriesMaxSize = stats.SeriesMaxSize
	}
	// Record which task produced this block. The downsampled block inherited
	// its source's extensions, including any provenance of the source itself;
	// what matters is who produced the block that now exists.
	outMeta.Thanos.Extensions, err = w.provenance(task).For(outID, task.SourceBlocks).Stamp(outMeta.Thanos.Extensions)
	if err != nil {
		return nil, err
	}
	if err := outMeta.WriteToDir(w.logger, resdir); err != nil {
		return nil, errors.Wrap(err, "write meta")
	}

	// Last chance to find out somebody else took this task over.
	if preUploadCheck != nil {
		if err := preUploadCheck(ctx); err != nil {
			return nil, err
		}
	}

	begin = time.Now()
	if err := block.Upload(ctx, w.logger, w.bkt, resdir, metadata.HashFunc(task.Group.HashFunc)); err != nil {
		return nil, compact.NewRetryError(errors.Wrapf(err, "upload downsampled block %s", outID))
	}
	w.m.stageDuration.WithLabelValues("upload").Observe(time.Since(begin).Seconds())
	level.Info(w.logger).Log("msg", "uploaded block", "id", outID, "duration", time.Since(begin))

	if err := os.RemoveAll(bdir); err != nil {
		level.Warn(w.logger).Log("msg", "failed to clean directory", "dir", bdir, "err", err)
	}
	if err := os.RemoveAll(resdir); err != nil {
		level.Warn(w.logger).Log("msg", "failed to clean directory", "dir", resdir, "err", err)
	}

	return []ulid.ULID{outID}, nil
}

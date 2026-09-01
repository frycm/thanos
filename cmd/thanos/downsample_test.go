// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestStuckBlockDownsamplingFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "default"},
		{name: "enabled", args: []string{"--downsampling.enable-stuck-blocks"}, want: true},
		{name: "disabled", args: []string{"--no-downsampling.enable-stuck-blocks"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := kingpin.New("thanos", "test")
			var compactConf compactConfig
			compactConf.registerFlag(app.Command("compact", ""))
			_, err := app.Parse(append([]string{"compact"}, tc.args...))
			testutil.Ok(t, err)
			testutil.Equals(t, compactModeStandalone, compactConf.mode)
			testutil.Equals(t, false, compactConf.disableDownsampling)
			testutil.Equals(t, tc.want, compactConf.enableStuckBlockDownsampling)

			app = kingpin.New("thanos", "test")
			var bucketConf bucketDownsampleConfig
			bucketConf.registerBucketDownsampleFlag(app.Command("downsample", ""))
			_, err = app.Parse(append([]string{"downsample"}, tc.args...))
			testutil.Ok(t, err)
			testutil.Equals(t, tc.want, bucketConf.enableStuckBlockDownsampling)
		})
	}
}

// Exercise the standalone compactor's dispatch boundary with a real short
// block: the default policy uploads nothing; opting in produces a 5m block.
func TestRunDownsamplingStuckBlocksOptIn(t *testing.T) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	dir := t.TempDir()
	id, err := e2eutil.CreateBlock(ctx, dir,
		[]labels.Labels{labels.FromStrings("a", "1")}, 20, 0, time.Hour.Milliseconds(),
		labels.FromStrings("tenant", "t1"), downsample.ResLevel0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
	meta, err := block.DownloadMeta(ctx, logger, bkt, id)
	testutil.Ok(t, err)
	metas := map[ulid.ULID]*metadata.Meta{id: &meta}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		id: {ID: id, Version: metadata.NoCompactMarkVersion1, Reason: metadata.IndexSizeExceedingNoCompactReason},
	}
	metrics := newDownsampleMetrics(prometheus.NewRegistry())
	conf := compactConfig{downsampleConcurrency: 1, blockFilesConcurrency: 1}
	testutil.Ok(t, runDownsampling(ctx, logger, nil, metrics, bkt, metas, marks, nil, t.TempDir(), conf))
	testutil.Equals(t, 0.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(meta.Thanos.ResolutionString())))

	conf.enableStuckBlockDownsampling = true
	testutil.Ok(t, runDownsampling(ctx, logger, nil, metrics, bkt, metas, marks, nil, t.TempDir(), conf))
	testutil.Equals(t, 1.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(meta.Thanos.ResolutionString())))
	var outputs int
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error {
		out, ok := block.IsBlockDir(name)
		if !ok || out == id {
			return nil
		}
		m, err := block.DownloadMeta(ctx, logger, bkt, out)
		testutil.Ok(t, err)
		testutil.Equals(t, downsample.ResLevel1, m.Thanos.Downsample.Resolution)
		testutil.Equals(t, []ulid.ULID{id}, m.Compaction.Sources)
		outputs++
		return nil
	}))
	testutil.Equals(t, 1, outputs)
}

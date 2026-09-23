// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

// TestCompactorProcess is the subprocess entry point of the process tests: it
// enters the actual Thanos main with its own flags, HTTP server, signal
// handling and actor lifecycle. No Docker or separately built binary is
// required, so these checks run in the normal Go test suite as well as CI.
func TestCompactorProcess(t *testing.T) {
	raw := os.Getenv("THANOS_TEST_COMPONENT_ARGS")
	if raw == "" {
		t.Skip("subprocess entry point")
	}
	var args []string
	testutil.Ok(t, json.Unmarshal([]byte(raw), &args))
	os.Args = append([]string{"thanos"}, args...)
	main()
}

// compactorProcessFixture is a set of filesystem buckets holding the same HA
// blocks, one per compactor configuration a process test wants to compare,
// and the plumbing to run the real binary against them.
type compactorProcessFixture struct {
	t       *testing.T
	ctx     context.Context
	Buckets []objstore.Bucket
	Dirs    []string
}

type compactorProcess struct {
	done <-chan error
	log  string
}

func newCompactorProcessFixture(t *testing.T, ctx context.Context, buckets int) *compactorProcessFixture {
	t.Helper()
	logger := log.NewNopLogger()
	f := &compactorProcessFixture{t: t, ctx: ctx}
	for range buckets {
		dir := t.TempDir()
		bkt, err := filesystem.NewBucket(dir)
		testutil.Ok(t, err)
		t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })
		f.Buckets = append(f.Buckets, bkt)
		f.Dirs = append(f.Dirs, dir)
	}
	prepare := t.TempDir()
	series := []labels.Labels{
		labels.FromStrings("__name__", "prom_metric", "prometheus_replica", "A"),
		labels.FromStrings("__name__", "prom_metric", "prometheus_replica", "B"),
		labels.FromStrings("__name__", "otel_metric", "otelcol_replica", "A"),
		labels.FromStrings("__name__", "otel_metric", "otelcol_replica", "B"),
	}
	for window := range 5 {
		for _, receiver := range []string{"r0", "r1"} {
			mint := int64(window) * 2 * time.Hour.Milliseconds()
			id, err := e2eutil.CreateBlock(ctx, prepare, series, 20, mint, mint+2*time.Hour.Milliseconds(),
				labels.FromStrings("tenant", "one", "receiver_replica", receiver), 0, metadata.NoneFunc, nil)
			testutil.Ok(t, err)
			for _, bkt := range f.Buckets {
				testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(prepare, id.String()), metadata.NoneFunc))
			}
		}
	}
	return f
}

// Args are the compact command's flags for a bucket, with the deduplication
// configuration of the HA deployment the fixture models.
func (f *compactorProcessFixture) Args(bucketDir, address string) []string {
	f.t.Helper()
	config := filepath.Join(f.t.TempDir(), "bucket.yaml")
	testutil.Ok(f.t, os.WriteFile(config, []byte(fmt.Sprintf("type: FILESYSTEM\nconfig:\n  directory: %q\n", bucketDir)), 0600))
	args := []string{"compact", "--objstore.config-file=" + config, "--data-dir=" + f.t.TempDir(), "--http-address=" + address,
		"--consistency-delay=0s", "--compact.progress-interval=0s", "--deduplication.func=penalty", "--web.disable"}
	for _, label := range []string{"prometheus_replica", "receiver_replica", "otelcol_replica", "ruler_replica"} {
		args = append(args, "--deduplication.replica-label="+label)
	}
	return args
}

// Start runs the binary with the arguments as a subprocess.
func (f *compactorProcessFixture) Start(args []string) compactorProcess {
	f.t.Helper()
	body, err := json.Marshal(args)
	testutil.Ok(f.t, err)
	cmd := exec.CommandContext(f.ctx, os.Args[0], "-test.run=^TestCompactorProcess$")
	cmd.Env = append(os.Environ(), "THANOS_TEST_COMPONENT_ARGS="+string(body))
	logfile := filepath.Join(f.t.TempDir(), "process.log")
	out, err := os.Create(logfile)
	testutil.Ok(f.t, err)
	cmd.Stdout, cmd.Stderr = out, out
	testutil.Ok(f.t, cmd.Start())
	done := make(chan error, 1)
	go func() { err := cmd.Wait(); _ = out.Close(); done <- err; close(done) }()
	f.t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
	return compactorProcess{done: done, log: logfile}
}

// Wait blocks until the process exits and fails the test if it did not succeed.
func (f *compactorProcessFixture) Wait(p compactorProcess) {
	f.t.Helper()
	select {
	case err := <-p.done:
		if err != nil {
			output, _ := os.ReadFile(p.log)
			f.t.Fatalf("Thanos process failed: %v\n%s", err, output)
		}
	case <-f.ctx.Done():
		output, _ := os.ReadFile(p.log)
		f.t.Fatalf("Thanos process timed out: %v\n%s", f.ctx.Err(), output)
	}
}

// TestCompactorStandaloneProcess runs the real compact command once over the
// fixture and checks that it compacted and that what it left behind is what
// an in-process compaction of the same blocks yields.
func TestCompactorStandaloneProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	f := newCompactorProcessFixture(t, ctx, 2)
	f.Wait(f.Start(f.Args(f.Dirs[0], "127.0.0.1:0")))
	got := processBucketSamples(t, f.Buckets[0])
	testutil.Assert(t, len(got) > 0, "the compactor left no samples behind")

	// A second run over an identical bucket must be reproducible.
	f.Wait(f.Start(f.Args(f.Dirs[1], "127.0.0.1:0")))
	testutil.Equals(t, got, processBucketSamples(t, f.Buckets[1]), "two standalone runs over the same blocks must serve the same samples")
}

// processBucketSamples reads every sample a store gateway would serve from
// the bucket, keyed by external and series labels, and asserts that some
// compaction happened.
func processBucketSamples(t *testing.T, bkt objstore.Bucket) map[string][]string {
	t.Helper()
	ctx := t.Context()
	logger := log.NewNopLogger()
	instrumented := objstore.WithNoopInstr(bkt)
	fetcher, err := block.NewMetaFetcher(logger, 1, instrumented, block.NewConcurrentLister(logger, instrumented), "", nil,
		[]block.MetadataFilter{block.NewIgnoreDeletionMarkFilter(logger, instrumented, 0, 1), block.NewDeduplicateFilter(1)})
	testutil.Ok(t, err)
	metas, _, err := fetcher.Fetch(ctx)
	testutil.Ok(t, err)
	result := map[string][]string{}
	compacted := 0
	for id, m := range metas {
		if m.Compaction.Level > 1 {
			compacted++
		}
		dir := filepath.Join(t.TempDir(), id.String())
		testutil.Ok(t, block.Download(ctx, logger, bkt, id, dir))
		b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), dir, nil, nil)
		testutil.Ok(t, err)
		ir, err := b.Index()
		testutil.Ok(t, err)
		cr, err := b.Chunks()
		testutil.Ok(t, err)
		name, value := index.AllPostingsKey()
		postings, err := ir.Postings(ctx, name, value)
		testutil.Ok(t, err)
		for postings.Next() {
			var builder labels.ScratchBuilder
			var chks []chunks.Meta
			testutil.Ok(t, ir.Series(postings.At(), &builder, &chks))
			key := labels.FromMap(m.Thanos.Labels).String() + builder.Labels().String()
			for _, cm := range chks {
				ch, _, err := cr.ChunkOrIterable(cm)
				testutil.Ok(t, err)
				it := ch.Iterator(nil)
				for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
					testutil.Equals(t, chunkenc.ValFloat, typ)
					ts, v := it.At()
					result[key] = append(result[key], fmt.Sprintf("%d/%016x", ts, math.Float64bits(v)))
				}
				testutil.Ok(t, it.Err())
			}
		}
		testutil.Ok(t, postings.Err())
		testutil.Ok(t, cr.Close())
		testutil.Ok(t, ir.Close())
		testutil.Ok(t, b.Close())
	}
	testutil.Assert(t, compacted > 0, "each mode must actually compact blocks")
	for _, values := range result {
		slices.Sort(values)
	}
	return result
}

// TestCompactorProcessSeriesReplicaDedupWithoutExternalReplicaLabels runs the
// real compact command with penalty deduplication by a series replica label
// alone - HA Prometheus pairs remote-writing to receivers that replicate
// nothing, so the replicas differ only in a series label. The command must
// start, and the compacted blocks must hold each Prometheus pair's series
// once, without the label, recording it.
func TestCompactorProcessSeriesReplicaDedupWithoutExternalReplicaLabels(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	f := newCompactorProcessFixture(t, ctx, 1)
	config := filepath.Join(t.TempDir(), "bucket.yaml")
	testutil.Ok(t, os.WriteFile(config, []byte(fmt.Sprintf("type: FILESYSTEM\nconfig:\n  directory: %q\n", f.Dirs[0])), 0600))
	f.Wait(f.Start([]string{"compact", "--objstore.config-file=" + config, "--data-dir=" + t.TempDir(), "--http-address=127.0.0.1:0",
		"--consistency-delay=0s", "--compact.progress-interval=0s", "--web.disable",
		"--deduplication.func=penalty", "--deduplication.series-replica-label=prometheus_replica"}))

	got := processBucketSamples(t, f.Buckets[0])
	for _, receiver := range []string{"r0", "r1"} {
		ext := labels.FromStrings("receiver_replica", receiver, "tenant", "one").String()
		_, deduplicated := got[ext+`{__name__="prom_metric"}`]
		testutil.Assert(t, deduplicated, "receiver %s's stream holds no deduplicated prom_metric: %v", receiver, slices.Collect(maps.Keys(got)))
		_, otel := got[ext+`{__name__="otel_metric", otelcol_replica="A"}`]
		testutil.Assert(t, otel, "a label not listed must be left alone")
	}

	logger := log.NewNopLogger()
	metas, _, err := func() (map[ulid.ULID]*metadata.Meta, map[ulid.ULID]error, error) {
		instrumented := objstore.WithNoopInstr(f.Buckets[0])
		fetcher, err := block.NewMetaFetcher(logger, 1, instrumented, block.NewConcurrentLister(logger, instrumented), "", nil, nil)
		testutil.Ok(t, err)
		return fetcher.Fetch(ctx)
	}()
	testutil.Ok(t, err)
	var recorded int
	for _, m := range metas {
		if m.Compaction.Level > 1 {
			testutil.Equals(t, []string{"prometheus_replica"}, m.Thanos.SeriesReplicaLabels)
			recorded++
		}
	}
	testutil.Assert(t, recorded > 0, "no compacted block")
}

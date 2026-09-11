// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
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

// A subprocess enters the actual Thanos main with its own flags, HTTP server,
// signal handling and actor lifecycle. No Docker or separately built binary is
// required, so this check runs in the normal Go test suite as well as CI.
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

func TestCompactorManagerWorkerProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	logger := log.NewNopLogger()
	bucketDirs := []string{t.TempDir(), t.TempDir()}
	buckets := make([]objstore.Bucket, 0, len(bucketDirs))
	for _, dir := range bucketDirs {
		bkt, err := filesystem.NewBucket(dir)
		testutil.Ok(t, err)
		buckets = append(buckets, bkt)
		t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })
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
			for _, bkt := range buckets {
				testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(prepare, id.String()), metadata.NoneFunc))
			}
		}
	}
	argsFor := func(bucketDir, address string) []string {
		config := filepath.Join(t.TempDir(), "bucket.yaml")
		testutil.Ok(t, os.WriteFile(config, []byte(fmt.Sprintf("type: FILESYSTEM\nconfig:\n  directory: %q\n", bucketDir)), 0600))
		args := []string{"compact", "--objstore.config-file=" + config, "--data-dir=" + t.TempDir(), "--http-address=" + address,
			"--consistency-delay=0s", "--compact.progress-interval=0s", "--deduplication.func=penalty", "--web.disable"}
		for _, label := range []string{"prometheus_replica", "receiver_replica", "otelcol_replica", "ruler_replica"} {
			args = append(args, "--deduplication.replica-label="+label)
		}
		return args
	}
	type process struct {
		done <-chan error
		log  string
	}
	start := func(args []string) process {
		t.Helper()
		body, err := json.Marshal(args)
		testutil.Ok(t, err)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCompactorProcess$")
		cmd.Env = append(os.Environ(), "THANOS_TEST_COMPONENT_ARGS="+string(body))
		logfile := filepath.Join(t.TempDir(), "process.log")
		out, err := os.Create(logfile)
		testutil.Ok(t, err)
		cmd.Stdout, cmd.Stderr = out, out
		testutil.Ok(t, cmd.Start())
		done := make(chan error, 1)
		go func() { err := cmd.Wait(); _ = out.Close(); done <- err; close(done) }()
		t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
		return process{done: done, log: logfile}
	}
	wait := func(p process) {
		t.Helper()
		select {
		case err := <-p.done:
			if err != nil {
				output, _ := os.ReadFile(p.log)
				t.Fatalf("Thanos process failed: %v\n%s", err, output)
			}
		case <-ctx.Done():
			output, _ := os.ReadFile(p.log)
			t.Fatalf("Thanos process timed out: %v\n%s", ctx.Err(), output)
		}
	}
	// Omitting compact.mode must keep the production standalone behavior.
	wait(start(argsFor(bucketDirs[0], "127.0.0.1:0")))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.Ok(t, err)
	address := listener.Addr().String()
	testutil.Ok(t, listener.Close())
	managerArgs := append(argsFor(bucketDirs[1], address), "--compact.mode=manager", "--compact.manager.journal-id=process-test")
	manager := start(managerArgs)
	for _, worker := range []string{"one", "two"} {
		start(append(argsFor(bucketDirs[1], "127.0.0.1:0"), "--compact.mode=worker", "--compact.manager.journal-id=process-test", "--compact.worker.id="+worker,
			"--compact.worker.manager-address="+address, "--compact.worker.poll-interval=25ms", "--compact.worker.heartbeat-interval=25ms"))
	}
	wait(manager)
	want := processBucketSamples(t, buckets[0])
	got := processBucketSamples(t, buckets[1])
	testutil.Assert(t, len(want) > 0, "the standalone reference must not be empty")
	testutil.Equals(t, want, got, "real manager/worker processes must preserve every standalone sample")
}

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

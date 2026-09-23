// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/compact/compacttest"
)

// seriesReplicaLabels are the replica labels the HA corpus writes inside its
// series: Prometheus pairs and collector pairs behind a receiver.
var seriesReplicaLabels = []string{"otelcol_replica", "prometheus_replica"}

func withSeriesDedup(c compacttest.NodeConfig) compacttest.NodeConfig {
	c.SeriesReplicaLabels = seriesReplicaLabels
	return c
}

func withoutSeriesDedup(c compacttest.NodeConfig) compacttest.NodeConfig {
	c.SeriesReplicaLabels = nil
	return c
}

// TestSeriesDedupScenarios runs the generic fault scenarios with series
// replica deduplication on, judged against the standalone compactor with it
// on, and checks that what the result serves is what a querier deduplicating
// by the same labels served before.
func TestSeriesDedupScenarios(t *testing.T) {
	compacttest.RunSuite(t, compacttest.Suite{
		Conf:      withSeriesDedup(compacttest.HANodeConfig()),
		Scenarios: seriesDedupScenarios(),
	})
}

// offsetScrapeCorpus is the HA corpus with the second replica of every
// Prometheus and collector pair scraping twenty seconds after the first, as
// HA replicas do.
func offsetScrapeCorpus() []compacttest.TenantSpec {
	tenants := compacttest.HACorpus()
	for i := range tenants {
		if len(tenants[i].PromReplicas) > 1 {
			tenants[i].ReplicaScrapeOffset = 20 * time.Second
		}
	}
	return tenants
}

// servesWhatQueryTimeDedupServed checks that the deduplicated bucket and a
// bucket compacted without series deduplication serve the same data to a
// querier that deduplicates by the replica labels with the penalty
// algorithm, reading the whole range at once or, with a window, one window
// at a time: for every series, some order of the replicas yields the same
// whole sequence from both. And that the deduplication really happened:
// every downsampled series - made from compacted, and so deduplicated, raw
// blocks - lacks the labels, and the compacted blocks record them.
func servesWhatQueryTimeDedupServed(window time.Duration) func(t *testing.T, r *compacttest.Run, want, _ *compacttest.BucketDump) {
	return func(t *testing.T, r *compacttest.Run, want, _ *compacttest.BucketDump) {
		got := compacttest.Converged(t, r, want)
		plain := compacttest.Golden(t, r.Corpus, withoutSeriesDedup(r.Conf))
		compacttest.AssertSameDeduplicated(t, plain, got, r.Conf.DedupReplicaLabels, window, "series-deduplicated against plain compaction")
		assertDeduplicated(t, plain, got)
	}
}

func assertDeduplicated(t *testing.T, plain, got *compacttest.BucketDump) {

	carrying := func(d *compacttest.BucketDump, downsampledOnly bool) int {
		n := 0
		for k := range d.Series {
			if downsampledOnly && strings.HasPrefix(k, "res=0 ") {
				continue
			}
			if slices.ContainsFunc(seriesReplicaLabels, func(l string) bool { return strings.Contains(k, l+"=") }) {
				n++
			}
		}
		return n
	}
	testutil.Assert(t, carrying(plain, true) > 0, "the plain run downsampled no replicated series; the corpus does not exercise anything")
	testutil.Equals(t, 0, carrying(got, true), "downsampled series still carry a series replica label")
	testutil.Assert(t, carrying(got, false) < carrying(plain, false), "no series replica was merged: %d series carry a replica label, %d without deduplication", carrying(got, false), carrying(plain, false))

	var recorded int
	for _, b := range got.Blocks {
		if len(b.Meta.Thanos.SeriesReplicaLabels) > 0 {
			testutil.Equals(t, seriesReplicaLabels, b.Meta.Thanos.SeriesReplicaLabels)
			recorded++
		}
	}
	testutil.Assert(t, recorded > 0, "no served block records the labels it was deduplicated by")
}

func seriesDedupScenarios() []compacttest.Scenario {
	return []compacttest.Scenario{
		{
			// Replicas in lockstep: every tie between them is at the same
			// timestamp and goes to the replica offered first, and the
			// compaction serves what a querier reading the whole range
			// serves, downsampled aggregates included.
			Name: "series_dedup_serves_what_query_time_dedup_served",
			Run:  servesWhatQueryTimeDedupServed(0),
		},
		{
			// Replicas scraping 20s apart, as real HA pairs do: penalty
			// deduplication keeps one replica's timestamps and drops the
			// other's. The compactor deduplicates each block window's
			// overlapping chunks on its own and serves, window by window,
			// what a querier reading that window alone serves - including
			// the later replica's first sample of the window, which a
			// querier reading across windows drops.
			Name:    "series_dedup_with_offset_scrapes_serves_per_window_what_query_time_dedup_served",
			Tenants: offsetScrapeCorpus(),
			Run:     servesWhatQueryTimeDedupServed(compacttest.Window),
		},
	}
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact_test

import (
	"slices"
	"strings"
	"testing"

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

func seriesDedupScenarios() []compacttest.Scenario {
	return []compacttest.Scenario{
		{
			// The deduplicated bucket and a bucket compacted without series
			// deduplication serve the same data to a querier that
			// deduplicates by the replica labels, as it has to today: the
			// same series at the same timestamps, every sample one a replica
			// really has. And the deduplication really happened: every
			// downsampled series - made from compacted, and so deduplicated,
			// raw blocks - lacks the labels, and the compacted blocks record
			// them.
			Name: "series_dedup_serves_what_query_time_dedup_served",
			Run: func(t *testing.T, r *compacttest.Run, want, _ *compacttest.BucketDump) {
				got := compacttest.Converged(t, r, want)
				plain := compacttest.Golden(t, r.Corpus, withoutSeriesDedup(r.Conf))
				compacttest.AssertSameDeduplicated(t, plain, got, r.Conf.DedupReplicaLabels, "series-deduplicated against plain compaction")

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
			},
		},
	}
}

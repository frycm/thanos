// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import (
	"fmt"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// TestDeduplicatedDifferences checks the comparison the series-dedup
// scenarios rely on: it accepts what penalty deduplication yields for some
// replica order, and rejects what it yields for none - an output alternating
// between healthy replicas, or the union of offset scrapes.
func TestDeduplicatedDifferences(t *testing.T) {
	series := func(from, step, n int64, v float64) []Sample {
		out := make([]Sample, 0, n)
		for i := range n {
			out = append(out, Sample{T: from + i*step, V: v + float64(i)})
		}
		return out
	}
	dump := func(replicas map[string][]Sample) *BucketDump {
		d := NewBucketDump()
		for replica, samples := range replicas {
			lset := labels.FromStrings("__name__", "up", "tenant", "a")
			if replica != "" {
				lset = labels.NewBuilder(lset).Set("prometheus_replica", replica).Labels()
			}
			key := fmt.Sprintf("res=0 ext={} series=%s", lset)
			d.Series[key] = map[downsample.AggrType][]Sample{AggrRaw: samples}
			d.keys[key] = dumpKey{res: 0, lset: lset}
		}
		return d
	}
	replicaLabels := []string{"prometheus_replica"}

	// Twenty synchronized samples at 15s intervals, values apart per replica.
	syncA, syncB := series(0, 15000, 20, 100), series(0, 15000, 20, 200)
	synced := map[string][]Sample{"A": syncA, "B": syncB}
	alternating := make([]Sample, len(syncA))
	for i := range syncA {
		alternating[i] = syncA[i]
		if i%2 == 1 {
			alternating[i] = syncB[i]
		}
	}

	// Offset scrapes: B five seconds after A. Penalty deduplication keeps one
	// replica and drops the other's timestamps; the union of both is not
	// what it serves.
	offsetB := series(5000, 15000, 20, 200)
	offset := map[string][]Sample{"A": syncA, "B": offsetB}
	union := append(append([]Sample{}, syncA...), offsetB...)
	duplicated := append(append([]Sample{}, syncA[:10]...), syncA[9:]...)

	// Offset by 20s at a 60s step, more than the algorithm's initial
	// penalty: read from the start of a window, the querier takes A's first
	// sample, then B's, and stays with B. A querier reading across two
	// windows stays with B at the second; one reading each window alone -
	// as a compaction deduplicating window by window does - takes A's first
	// sample of the second window again.
	slowA, slowB := series(0, 60000, 40, 100), series(20000, 60000, 40, 200)
	slow := map[string][]Sample{"A": slowA, "B": slowB}
	perWindow := []Sample{slowA[0]}
	perWindow = append(perWindow, slowB[0:20]...)
	perWindow = append(perWindow, slowA[20])
	perWindow = append(perWindow, slowB[20:]...)
	across := append([]Sample{slowA[0]}, slowB...)

	for _, tc := range []struct {
		name      string
		want, got map[string][]Sample
		window    time.Duration
		wantMatch bool
	}{
		{name: "replica A alone is what A-first yields", want: synced, got: map[string][]Sample{"": syncA}, wantMatch: true},
		{name: "replica B alone is what B-first yields", want: synced, got: map[string][]Sample{"": syncB}, wantMatch: true},
		{name: "the replicas themselves, deduplicated at query time", want: synced, got: synced, wantMatch: true},
		{name: "an output alternating between healthy replicas must be rejected", want: synced, got: map[string][]Sample{"": alternating}},
		{name: "replica A alone is what A-first yields for offset scrapes", want: offset, got: map[string][]Sample{"": syncA}, wantMatch: true},
		{name: "the union of offset scrapes must be rejected", want: offset, got: map[string][]Sample{"": union}},
		{name: "a lost sample must be rejected", want: offset, got: map[string][]Sample{"": syncA[:19]}},
		{name: "a duplicated sample must be rejected", want: offset, got: map[string][]Sample{"": duplicated}},
		{name: "read across windows", want: slow, got: map[string][]Sample{"": across}, wantMatch: true},
		{name: "read across windows, the querier drops B's samples", want: slow, got: map[string][]Sample{"": perWindow}},
		{name: "read window by window, it keeps B's first", want: slow, got: map[string][]Sample{"": perWindow}, window: 20 * time.Minute, wantMatch: true},
		{name: "windows do not excuse alternation", want: slow, got: map[string][]Sample{"": alternating}, window: 20 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diffs := deduplicatedDifferences(t, dump(tc.want), dump(tc.got), replicaLabels, tc.window)
			if tc.wantMatch {
				testutil.Equals(t, 0, len(diffs), "unexpected differences: %v", diffs)
				return
			}
			testutil.Assert(t, len(diffs) > 0, "differences expected")
		})
	}
}

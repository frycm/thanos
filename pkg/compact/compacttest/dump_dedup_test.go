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
		var out []Sample
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
	a, b := series(0, 15000, 20, 100), series(0, 15000, 20, 200)
	want := dump(map[string][]Sample{"A": a, "B": b})
	testutil.Equals(t, 0, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": a}), replicaLabels, 0)), "replica A alone is what A-first yields")
	testutil.Equals(t, 0, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": b}), replicaLabels, 0)), "replica B alone is what B-first yields")
	testutil.Equals(t, 0, len(deduplicatedDifferences(t, want, want, replicaLabels, 0)), "the replicas themselves, deduplicated at query time")

	alternating := make([]Sample, len(a))
	for i := range a {
		alternating[i] = a[i]
		if i%2 == 1 {
			alternating[i] = b[i]
		}
	}
	testutil.Assert(t, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": alternating}), replicaLabels, 0)) > 0, "an output alternating between healthy replicas must be rejected")

	// Offset scrapes: B five seconds after A. Penalty deduplication keeps one
	// replica and drops the other's timestamps; the union of both is not
	// what it serves.
	b = series(5000, 15000, 20, 200)
	want = dump(map[string][]Sample{"A": a, "B": b})
	testutil.Equals(t, 0, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": a}), replicaLabels, 0)))
	union := append(append([]Sample{}, a...), b...)
	testutil.Assert(t, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": union}), replicaLabels, 0)) > 0, "the union of offset scrapes must be rejected")
	testutil.Assert(t, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": a[:19]}), replicaLabels, 0)) > 0, "a lost sample must be rejected")
	duplicated := append(append([]Sample{}, a[:10]...), a[9:]...)
	testutil.Assert(t, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": duplicated}), replicaLabels, 0)) > 0, "a duplicated sample must be rejected")

	// Offset by 20s at a 60s step, more than the algorithm's initial
	// penalty: read from the start of a window, the querier takes A's first
	// sample, then B's, and stays with B. A querier reading across two
	// windows stays with B at the second; one reading each window alone -
	// as a compaction deduplicating window by window does - takes A's first
	// sample of the second window again.
	a, b = series(0, 60000, 40, 100), series(20000, 60000, 40, 200)
	want = dump(map[string][]Sample{"A": a, "B": b})
	perWindow := []Sample{a[0]}
	perWindow = append(perWindow, b[0:20]...)
	perWindow = append(perWindow, a[20])
	perWindow = append(perWindow, b[20:]...)
	got := dump(map[string][]Sample{"": perWindow})
	across := append([]Sample{a[0]}, b...)
	testutil.Equals(t, 0, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": across}), replicaLabels, 0)), "read across windows")
	testutil.Assert(t, len(deduplicatedDifferences(t, want, got, replicaLabels, 0)) > 0, "read across windows, the querier drops B's samples")
	diffs := deduplicatedDifferences(t, want, got, replicaLabels, 20*time.Minute)
	testutil.Equals(t, 0, len(diffs), "read window by window, it keeps B's first: %v", diffs)
	testutil.Assert(t, len(deduplicatedDifferences(t, want, dump(map[string][]Sample{"": alternating}), replicaLabels, 20*time.Minute)) > 0, "windows do not excuse alternation")
}

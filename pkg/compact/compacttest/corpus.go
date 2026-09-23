// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package compacttest is a fault scenario harness for the compactor: a
// synthetic block corpus, a content oracle for what a store gateway would
// serve from a bucket, a fault-injectable view of a bucket, and one compactor
// process wired the way cmd/thanos/compact.go wires it. It is shared by the
// standalone compactor's own scenario suite and by suites for features built
// on the compactor, so that every feature is judged against the same oracle.
//
// The oracle is content, not blocks. Two compactors may legitimately arrive at
// different block layouts; what must not differ is every series and sample at
// every resolution, per external label set.
package compacttest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

var runScenarios = flag.Bool("compact.scenarios", false,
	"Run the in-process fault scenario suites of the compactor. Slow; off by default.")

// SkipUnlessScenarios skips the test unless the scenario suites were asked for
// with -compact.scenarios or THANOS_COMPACT_SCENARIOS in the environment.
func SkipUnlessScenarios(t *testing.T) {
	t.Helper()
	if !*runScenarios && os.Getenv("THANOS_COMPACT_SCENARIOS") == "" {
		t.Skip("scenario suite disabled; run with -compact.scenarios or THANOS_COMPACT_SCENARIOS=1")
	}
}

// Window is the raw block length, as Prometheus and receive cut them.
const Window = 2 * time.Hour

// Levels are the compaction ranges every node plans with. A raw block is
// downsampled to 5m once it spans 40h, which the third level reaches.
var Levels = []int64{Window.Milliseconds(), (8 * time.Hour).Milliseconds(), (48 * time.Hour).Milliseconds()}

// TenantSpec describes one block stream of the corpus.
type TenantSpec struct {
	Name string
	// PromReplicas are the prometheus_replica values written as a series
	// label inside every block; empty means a single Prometheus.
	PromReplicas []string
	// SeriesReplicaLabel defaults to prometheus_replica; collectors use otelcol_replica.
	SeriesReplicaLabel string
	// ExternalReplicaLabel defaults to receiver_replica; rulers use ruler_replica.
	ExternalReplicaLabel string
	// Receivers are the receiver_replica values written as an external label,
	// one block per receiver per window; empty means no receive replication.
	Receivers []string
	Series    int
	Windows   int
	Samples   int
	// NoCompact marks the given windows' blocks no-compact for the reason.
	NoCompact              map[int]metadata.NoCompactReason
	SampleTypes            []chunkenc.ValueType
	MissingReceiverWindows bool
	// ReplicaScrapeOffset shifts the samples of the i-th PromReplica by i
	// times this much, as HA replicas scraping at different moments do.
	// Zero keeps the replicas in lockstep.
	ReplicaScrapeOffset time.Duration
}

// CorpusBlock is one uploaded block of the corpus.
type CorpusBlock struct {
	ID     ulid.ULID
	Dir    string
	Tenant string
	Window int
	Mark   metadata.NoCompactReason
}

// Corpus is a set of blocks built once and uploaded to every bucket a
// scenario needs, with the same IDs in each.
type Corpus struct {
	Name   string
	Blocks []CorpusBlock
}

// HACorpus is the default corpus: the deployment shape the suites exist for,
// plus a small plain tenant so that two groups are always in play.
func HACorpus() []TenantSpec {
	return []TenantSpec{
		// The planner never compacts the newest range, so both streams reach
		// one window past the range that has to be compacted: 48h for the HA
		// stream, which is what 5m downsampling needs, 8h for the plain one.
		{Name: "ha", MissingReceiverWindows: true, PromReplicas: []string{"A", "B"}, Receivers: []string{"r0", "r1"}, Series: 3, Windows: 25, Samples: 20},
		{Name: "otel", PromReplicas: []string{"A", "B"}, SeriesReplicaLabel: "otelcol_replica", Receivers: []string{"r0", "r1"}, Series: 2, Windows: 5, Samples: 20},
		{Name: "ruler", Receivers: []string{"r0", "r1"}, ExternalReplicaLabel: "ruler_replica", Series: 2, Windows: 5, Samples: 20},
		{Name: "histograms", Receivers: []string{"r0", "r1"}, Series: 2, Windows: 25, Samples: 10, SampleTypes: []chunkenc.ValueType{chunkenc.ValHistogram, chunkenc.ValFloatHistogram}},
		{Name: "plain", Series: 2, Windows: 5, Samples: 20},
	}
}

// BuildCorpus creates the blocks of the tenants on disk.
func BuildCorpus(t *testing.T, name string, tenants []TenantSpec) *Corpus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c := &Corpus{Name: name}
	dir := t.TempDir()
	for _, tn := range tenants {
		if tn.SeriesReplicaLabel == "" {
			tn.SeriesReplicaLabel = "prometheus_replica"
		}
		if tn.ExternalReplicaLabel == "" {
			tn.ExternalReplicaLabel = "receiver_replica"
		}
		receivers := tn.Receivers
		if len(receivers) == 0 {
			receivers = []string{""}
		}
		promReplicas := tn.PromReplicas
		if len(promReplicas) == 0 {
			promReplicas = []string{""}
		}
		var series []labels.Labels
		for _, pr := range promReplicas {
			for i := range tn.Series {
				b := labels.NewBuilder(labels.EmptyLabels())
				b.Set("__name__", fmt.Sprintf("metric_%d", i))
				b.Set("job", tn.Name)
				if pr != "" {
					b.Set(tn.SeriesReplicaLabel, pr)
				}
				series = append(series, b.Labels())
			}
		}
		for w := range tn.Windows {
			mint := int64(w) * Window.Milliseconds()
			maxt := mint + Window.Milliseconds()
			for _, rcv := range receivers {
				if tn.MissingReceiverWindows && rcv == "r1" && w%3 == 1 {
					continue
				}
				b := labels.NewBuilder(labels.EmptyLabels())
				b.Set("tenant", tn.Name)
				if rcv != "" {
					b.Set(tn.ExternalReplicaLabel, rcv)
				}
				var id ulid.ULID
				var err error
				if tn.ReplicaScrapeOffset > 0 {
					id, err = e2eutil.CreateBlockWithSampleOffsets(ctx, dir, series, tn.Samples, mint, maxt, b.Labels(), func(lset labels.Labels) int64 {
						return int64(slices.Index(promReplicas, lset.Get(tn.SeriesReplicaLabel))) * tn.ReplicaScrapeOffset.Milliseconds()
					})
				} else {
					id, err = e2eutil.CreateBlock(ctx, dir, series, tn.Samples, mint, maxt, b.Labels(), 0, metadata.NoneFunc, tn.SampleTypes)
				}
				testutil.Ok(t, err)
				c.Blocks = append(c.Blocks, CorpusBlock{
					ID: id, Dir: filepath.Join(dir, id.String()), Tenant: tn.Name, Window: w, Mark: tn.NoCompact[w],
				})
			}
		}
	}
	return c
}

// Upload puts the corpus into a bucket, marks included. The block IDs are the
// same in every bucket the corpus is uploaded to, which is what lets a run be
// checked against the input exactly.
func (c *Corpus) Upload(t *testing.T, bkt objstore.Bucket) {
	t.Helper()
	ctx := context.Background()
	logger := log.NewNopLogger()
	for _, b := range c.Blocks {
		testutil.Ok(t, block.Upload(ctx, logger, bkt, b.Dir, metadata.NoneFunc))
		if b.Mark != "" {
			testutil.Ok(t, block.MarkForNoCompact(ctx, logger, bkt, b.ID, b.Mark, "scenario corpus",
				promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "scenario_marks_total", Help: "Blocks the corpus marked no-compact."})))
		}
	}
}

// IDs returns the sorted block IDs of the corpus.
func (c *Corpus) IDs() []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(c.Blocks))
	for _, b := range c.Blocks {
		ids = append(ids, b.ID)
	}
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return ids
}

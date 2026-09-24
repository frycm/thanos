// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compacttest

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Scenarios are the faults every compactor has to survive, whatever executes
// its plans: they only touch the bucket, the process lifetime and the views,
// never a feature's own machinery. A feature's suite runs them against its
// own run, next to the scenarios for what it adds.
func Scenarios() []Scenario {
	scenarios := []Scenario{
		{
			Name: "golden_path",
			Run: func(t *testing.T, r *Run, want, _ *BucketDump) {
				Converged(t, r, want)
			},
		},
		{
			// The object store goes away once work is under way and comes
			// back a moment later. Nothing may be lost or duplicated.
			Name: "object_store_outage",
			Run: func(t *testing.T, r *Run, want, _ *BucketDump) {
				before := r.Fingerprint()
				r.Inject(func() {
					r.WaitFor("work to be under way", func() bool { return r.Fingerprint() != before })
					views := r.Views()
					lifts := make([]func(), 0, len(views))
					for _, v := range views {
						lifts = append(lifts, v.Outage())
					}
					time.Sleep(750 * time.Millisecond)
					for _, lift := range lifts {
						lift()
					}
				})
				Converged(t, r, want)
			},
		},
		{
			// The process is killed once work is under way and a new one
			// starts over the same bucket.
			Name: "process_crash_mid_run",
			Run: func(t *testing.T, r *Run, want, _ *BucketDump) {
				before := r.Fingerprint()
				r.Inject(func() {
					r.WaitFor("work to be under way", func() bool { return r.Fingerprint() != before })
					r.Replace(r.Conf)
				})
				Converged(t, r, want)
			},
		},
		{
			// A source block that cannot be read must stop its group without
			// harming it: every source still there and unmarked, nothing
			// produced for it. The other groups are not part of the claim.
			Name: "corrupted_source_fails_without_damage",
			Run: func(t *testing.T, r *Run, _, _ *BucketDump) {
				var damaged CorpusBlock
				for _, b := range r.Corpus.Blocks {
					if b.Tenant == "plain" {
						damaged = b
						break
					}
				}
				testutil.Ok(t, r.Shared.Upload(t.Context(), filepath.Join(damaged.ID.String(), block.IndexFilename), strings.NewReader("this is not an index")))

				// The plain group fails on every pass, as it would in the
				// binary; a few passes are enough to give it every chance.
				_ = r.Passes(3)

				corpusIDs := map[ulid.ULID]struct{}{}
				for _, b := range r.Corpus.Blocks {
					corpusIDs[b.ID] = struct{}{}
					if b.Tenant != "plain" {
						continue
					}
					testutil.Assert(t, Exists(t, r.Shared, filepath.Join(b.ID.String(), block.MetaFilename)), "source %s of the damaged group is gone", b.ID)
					testutil.Assert(t, !Exists(t, r.Shared, filepath.Join(b.ID.String(), metadata.DeletionMarkFilename)), "source %s of the damaged group was marked", b.ID)
				}
				for _, id := range BlockIDs(t, r.Shared) {
					if _, ok := corpusIDs[id]; ok {
						continue
					}
					m, err := block.DownloadMeta(t.Context(), log.NewNopLogger(), r.Shared, id)
					testutil.Ok(t, err)
					testutil.Assert(t, m.Thanos.Labels["tenant"] != "plain", "block %s was produced for the damaged group", id)
				}
			},
		},
	}

	// A failed publication of any block file, whether rejected before the
	// write or acknowledged too late, must leave the sources intact and be
	// retried into the same content as a clean run.
	for _, stage := range []string{"chunks", "index", "meta.json"} {
		for _, before := range []bool{true, false} {
			name := "publication_failure/" + stage + "/before-write"
			if !before {
				name = "publication_failure/" + stage + "/lost-acknowledgement"
			}
			scenarios = append(scenarios, Scenario{
				Name: name,
				Run: func(t *testing.T, r *Run, want, _ *BucketDump) {
					fault := &PublicationFault{Stage: stage, Before: before, Skip: isNotBlockFile}
					for _, v := range r.Views() {
						fault.Install(v)
					}
					sources := SourceObjects(t, r.Shared, r.Corpus.IDs())
					Converged(t, r, want)
					testutil.Assert(t, fault.Hits() > 0, "the fault never fired")
					testutil.Equals(t, sources, SourceObjects(t, r.Shared, r.Corpus.IDs()), "source blocks changed")
				},
			})
		}
	}
	return scenarios
}

// isNotBlockFile exempts everything that is not under a block directory,
// such as a journal, from publication faults.
func isNotBlockFile(name string) bool {
	first, _, ok := strings.Cut(name, "/")
	if !ok {
		return true
	}
	_, err := ulid.Parse(first)
	return err != nil
}

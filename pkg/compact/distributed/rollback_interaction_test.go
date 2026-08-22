// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// TestInteractionRollbackRestoresBucket is the rehearsal of reverting a trial:
// after a real compaction through the distributed stack, the rollback must
// leave the bucket exactly as the standalone compactor left it - the produced
// block gone, the sources back without their marks - and nothing else touched.
func TestInteractionRollbackRestoresBucket(t *testing.T) {
	c := newTestCluster(t)
	c.startWorker("w1")

	// A block the distributed compactor never touched, with a mark it did not
	// write, must survive the rollback untouched.
	bystander, bystanderMetas := c.makeGroup(labels.FromStrings("ext", "bystander"))
	_ = bystander
	testutil.Ok(t, block.MarkForDeletion(context.Background(), c.logger, c.shared, bystanderMetas[0].ULID, "retention",
		prometheus.NewCounter(prometheus.CounterOpts{Name: "test"})))

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	var got executeOutcome
	select {
	case got = <-c.execute(cg, toCompact):
	case <-time.After(30 * time.Second):
		t.Fatal("compaction did not finish")
	}
	testutil.Ok(t, got.err)
	produced := got.compIDs[0]

	// The state a trial leaves behind: result present, sources marked, and
	// the result recording what it was made from.
	testutil.Equals(t, true, exists(t, c.shared, filepath.Join(produced.String(), block.MetaFilename)))
	producedMeta, err := block.DownloadMeta(context.Background(), c.logger, c.shared, produced)
	testutil.Ok(t, err)
	prov, ok := ProvenanceOf(&producedMeta)
	testutil.Equals(t, true, ok)
	testutil.Equals(t, produced.String(), prov.BlockID)
	testutil.Equals(t, []string{toCompact[0].ULID.String(), toCompact[1].ULID.String()}, prov.Sources)
	for _, m := range toCompact {
		testutil.Equals(t, true, exists(t, c.shared, filepath.Join(m.ULID.String(), metadata.DeletionMarkFilename)))
	}

	plan, err := PlanRollback(context.Background(), c.logger, objstore.WithNoopInstr(c.shared), RollbackOptions{JournalID: journalID})
	testutil.Ok(t, err)
	testutil.Equals(t, []ulid.ULID{produced}, plan.Produced)
	testutil.Equals(t, 2, len(plan.Restore))

	// The manager of this cluster is still running, and the plan knows: this
	// is what makes the tool refuse to apply in production. The harness owns
	// the manager and knows it is idle, so it may proceed.
	testutil.Equals(t, []string{journalID}, plan.RecentlyActive(15*time.Minute, time.Now()))

	testutil.Ok(t, plan.Apply(context.Background(), c.logger, c.shared))

	// Back to where the standalone compactor left things.
	testutil.Equals(t, false, exists(t, c.shared, filepath.Join(produced.String(), block.MetaFilename)))
	for _, m := range toCompact {
		testutil.Equals(t, true, exists(t, c.shared, filepath.Join(m.ULID.String(), block.MetaFilename)))
		testutil.Equals(t, false, exists(t, c.shared, filepath.Join(m.ULID.String(), metadata.DeletionMarkFilename)))
	}
	// The bystander and its foreign mark are exactly as they were.
	testutil.Equals(t, true, exists(t, c.shared, filepath.Join(bystanderMetas[0].ULID.String(), block.MetaFilename)))
	testutil.Equals(t, true, exists(t, c.shared, filepath.Join(bystanderMetas[0].ULID.String(), metadata.DeletionMarkFilename)))
}

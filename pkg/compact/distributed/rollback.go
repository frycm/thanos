// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Rollback undoes everything the distributed compactor did to a bucket, so a
// trial can be reverted to the state the standalone compactor left behind.
//
// Worker provenance records each output and its immediate sources. A rollback
// restores those sources, including marks left by garbage collection after a
// manager crash, before deleting the outputs. It refuses to delete a replacement
// if any required original block is missing or has a foreign deletion mark.
//
// Restoration happens first, and outputs are deleted from descendants to
// ancestors. With every writer stopped, interruption at any point leaves either
// the replacement or its restored sources available and planning can be retried.
// Keep writers stopped until Apply completes.
//
// Run it only with the manager and every worker stopped, and before the
// compactor's delete delay has passed for the oldest marks: a source that was
// physically deleted cannot be restored.
type Rollback struct {
	// Produced are the blocks a worker made. They are deleted.
	Produced []ulid.ULID
	// Restore are the sources marked by the manager or garbage collection.
	// Their marks are removed before outputs are deleted.
	Restore []ulid.ULID

	// deleteOrder puts descendants before their produced source blocks.
	deleteOrder []ulid.ULID

	// Options the plan was built with.
	Options RollbackOptions

	// Unreadable lists blocks whose metadata could not be read. It is only ever
	// non-empty when the plan was built with AllowUnreadableBlocks; otherwise
	// planning fails instead, see PlanRollback.
	Unreadable []ulid.ULID

	// JournalsUpdatedAt is when each journal in scope was last written. A
	// running manager writes its journal at least once per lease TTL, so a
	// recent write means a manager may still be running.
	JournalsUpdatedAt map[string]time.Time
}

// RollbackOptions says whose work to undo and what to tolerate.
type RollbackOptions struct {
	// JournalID scopes the rollback to the work of one manager. Required
	// unless AllJournals is set.
	JournalID string
	// AllJournals undoes the work of every manager that ever wrote to the
	// bucket. In a bucket shared by several shards that is every shard's
	// trial at once, so it has to be asked for explicitly.
	AllJournals bool
	// AllowUnreadableBlocks lets planning proceed although the metadata of some
	// blocks could not be read. See PlanRollback for why that is refused by
	// default.
	AllowUnreadableBlocks bool
}

func (o RollbackOptions) validate() error {
	switch {
	case o.JournalID == "" && !o.AllJournals:
		return errors.New("a journal ID is required, or AllJournals to undo the work of every manager")
	case o.JournalID != "" && o.AllJournals:
		return errors.New("a journal ID and AllJournals are mutually exclusive")
	}
	return nil
}

func (o RollbackOptions) matches(journalID string) bool {
	return o.AllJournals || journalID == o.JournalID
}

// PlanRollback inspects the bucket and returns what a rollback would do,
// without changing anything.
//
// It refuses to plan on an incomplete picture. A block whose metadata cannot
// be read might be a source the manager marked for deletion: its replacement
// would be deleted while the source itself stayed marked, and the bucket would
// end up with neither. Unless told that the unreadable blocks are known to be
// unrelated - typically the remains of aborted uploads - planning fails and
// names them.
func PlanRollback(ctx context.Context, logger log.Logger, bkt objstore.InstrumentedBucket, opts RollbackOptions) (*Rollback, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// No filters: blocks marked for deletion are exactly the ones this has to
	// see, and the consistency delay does not apply to an offline inspection.
	fetcher, err := block.NewMetaFetcher(logger, 32, bkt, block.NewConcurrentLister(logger, bkt), "", prometheus.NewRegistry(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create meta fetcher")
	}
	metas, partial, err := fetcher.Fetch(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fetch block metadata")
	}

	r := &Rollback{Options: opts, JournalsUpdatedAt: map[string]time.Time{}}

	for id := range partial {
		r.Unreadable = append(r.Unreadable, id)
	}
	slices.SortFunc(r.Unreadable, func(a, b ulid.ULID) int { return a.Compare(b) })
	if len(r.Unreadable) > 0 && !opts.AllowUnreadableBlocks {
		return nil, errors.Errorf("the metadata of %d block(s) could not be read (%v); a rollback planned without them could delete a "+
			"replacement while leaving its source marked for deletion, so refusing. Repair or remove them first, or allow "+
			"unreadable blocks if they are known to be unrelated, such as the remains of aborted uploads",
			len(r.Unreadable), r.Unreadable)
	}
	for _, id := range r.Unreadable {
		level.Warn(logger).Log("msg", "planning without a block whose metadata could not be read", "block", id, "err", partial[id])
	}

	produced := map[ulid.ULID]Provenance{}
	for id, m := range metas {
		if p, ok := ProvenanceOf(m); ok && opts.matches(p.JournalID) {
			produced[id] = p
			r.Produced = append(r.Produced, id)
		}
	}

	marks := map[ulid.ULID]string{}
	for id := range metas {
		var mark metadata.DeletionMark
		if err := metadata.ReadMarker(ctx, logger, bkt, id.String(), &mark); err != nil {
			if errors.Is(err, metadata.ErrorMarkerNotFound) {
				continue
			}
			return nil, errors.Wrapf(err, "read deletion mark of %s", id)
		}
		marks[id] = mark.Details
	}
	removable := func(id ulid.ULID) bool {
		details, marked := marks[id]
		if !marked || details == outdatedBlockDetails {
			return true
		}
		journal, _, ok := ParseDeletionDetails(details)
		return ok && opts.matches(journal)
	}

	// Walk the immediate-source graph, not Compaction.Sources: the latter
	// names original raw ancestors that may have been deleted before the trial.
	restore := map[ulid.ULID]struct{}{}
	visiting, visited := map[ulid.ULID]bool{}, map[ulid.ULID]bool{}
	var recoverSource func(ulid.ULID) error
	recoverSource = func(id ulid.ULID) error {
		if visiting[id] {
			return errors.Errorf("source provenance contains a cycle at %s", id)
		}
		if visited[id] {
			return nil
		}
		if _, ok := metas[id]; !ok {
			return errors.Errorf("source %s is no longer in the bucket", id)
		}
		if p, ok := produced[id]; ok {
			if len(p.Sources) == 0 {
				return errors.Errorf("block %s records no immediate sources", id)
			}
			visiting[id] = true
			for _, raw := range p.Sources {
				src, err := ulid.Parse(raw)
				if err != nil {
					return errors.Wrapf(err, "invalid source %q of block %s", raw, id)
				}
				if err := recoverSource(src); err != nil {
					return err
				}
			}
			delete(visiting, id)
			r.deleteOrder = append(r.deleteOrder, id)
		} else {
			if !removable(id) {
				return errors.Errorf("source %s has a foreign deletion mark: %q", id, marks[id])
			}
			if _, marked := marks[id]; marked {
				restore[id] = struct{}{}
			}
		}
		visited[id] = true
		return nil
	}
	// A stable traversal makes dry runs and interruption tests reproducible.
	slices.SortFunc(r.Produced, func(a, b ulid.ULID) int { return a.Compare(b) })
	for _, id := range r.Produced {
		if err := recoverSource(id); err != nil {
			return nil, errors.Wrapf(err, "cannot restore sources of output %s; refusing rollback", id)
		}
	}
	slices.Reverse(r.deleteOrder)

	// Also restore marks left by tasks with no output (all sources empty), or
	// an earlier interrupted rollback whose outputs have already been deleted.
	for id, details := range marks {
		if _, ok := produced[id]; ok {
			continue
		}
		journal, _, ok := ParseDeletionDetails(details)
		if ok && opts.matches(journal) {
			restore[id] = struct{}{}
		}
	}
	for id := range restore {
		r.Restore = append(r.Restore, id)
	}

	slices.SortFunc(r.Produced, func(a, b ulid.ULID) int { return a.Compare(b) })
	slices.SortFunc(r.Restore, func(a, b ulid.ULID) int { return a.Compare(b) })

	// Every journal in scope is consulted for liveness, not only the one named:
	// undoing every manager's work means every manager has to be stopped.
	journalIDs := []string{opts.JournalID}
	if opts.AllJournals {
		journalIDs, err = listJournals(ctx, bkt)
		if err != nil {
			return nil, errors.Wrap(err, "list journals")
		}
	}
	for _, id := range journalIDs {
		j, err := ReadJournal(ctx, bkt, id)
		if err != nil {
			return nil, errors.Wrapf(err, "read journal %s", id)
		}
		if j != nil {
			r.JournalsUpdatedAt[id] = j.UpdatedAt
		}
	}
	return r, nil
}

// listJournals returns the IDs of every journal in the bucket.
func listJournals(ctx context.Context, bkt objstore.Bucket) ([]string, error) {
	var ids []string
	err := bkt.Iter(ctx, JournalPrefix+objstore.DirDelim, func(name string) error {
		name = strings.TrimPrefix(name, JournalPrefix+objstore.DirDelim)
		name = strings.TrimSuffix(name, objstore.DirDelim)
		if name != "" {
			ids = append(ids, name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return ids, nil
}

// RecentlyActive returns the journals in scope written within the window
// before now. A running manager writes its journal at least once per lease
// TTL, so with a window of a few TTLs a journal listed here may well belong to
// a manager that is still running - and a rollback must not race one.
func (r *Rollback) RecentlyActive(window time.Duration, now time.Time) []string {
	var active []string
	for id, at := range r.JournalsUpdatedAt {
		if now.Sub(at) < window {
			active = append(active, id)
		}
	}
	slices.Sort(active)
	return active
}

// Apply restores sources before deleting outputs. All writers must stay stopped
// until it returns successfully, including while retrying an interrupted rollback.
func (r *Rollback) Apply(ctx context.Context, logger log.Logger, bkt objstore.Bucket) error {

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_rollback_marks_removed_total",
		Help: "Total number of deletion marks removed by the rollback.",
	})
	for _, id := range r.Restore {
		level.Info(logger).Log("msg", "restoring block marked for deletion by the distributed compactor", "block", id)
		if err := block.RemoveMark(ctx, logger, bkt, id, counter, metadata.DeletionMarkFilename); err != nil {
			return errors.Wrapf(err, "remove deletion mark of %s", id)
		}
	}
	for _, id := range r.deleteOrder {
		level.Info(logger).Log("msg", "deleting block produced by the distributed compactor", "block", id)
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			return errors.Wrapf(err, "delete produced block %s", id)
		}
	}

	return nil
}

// outdatedBlockDetails is the mark written by compact.Syncer.GarbageCollect.
const outdatedBlockDetails = "outdated block"

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"fmt"
	"maps"
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
// It relies on two things every worker and manager record as they go: blocks
// produced by a worker carry Provenance in their extensions, and deletion marks
// written by a manager carry DeletionDetails. Nothing else in the bucket is
// touched. Both are needed: the produced blocks have to go, and the sources
// they replaced have to come back, or the bucket would hold neither.
//
// Order matters. The produced blocks are deleted first, and by their meta.json
// first, so they vanish from every reader's view before the sources return.
// Were the sources restored while the produced blocks were still visible, a
// restarted standalone compactor would see the produced blocks cover them and
// mark the sources as duplicates all over again.
//
// Deleting a produced block is only safe when what it replaced can be brought
// back. A block a worker made records the blocks its task consumed, and the
// plan checks every one of them: it has to be in the bucket still, and either
// carry no deletion mark, or one the rollback removes. A source that was
// physically deleted after the delete delay passed, or that retention or an
// operator marked, cannot be relied on, and deleting the block made from it
// would lose the data for good. Such blocks are refused by default, see
// RollbackOptions.Unrecoverable.
//
// Run it only with the manager and every worker stopped, and before the
// compactor's delete delay has passed for the oldest marks: a source that was
// physically deleted cannot be restored.
type Rollback struct {
	// Produced are the blocks a worker made. They are deleted.
	Produced []ulid.ULID
	// Restore are the blocks the produced blocks replaced. Their marks are
	// removed: those the manager wrote, and those the standalone compactor's
	// garbage collection wrote on a source it found covered by a produced
	// block before the manager could mark it itself.
	Restore []ulid.ULID

	// Kept are blocks a worker made whose sources cannot be brought back, left
	// in the bucket because the plan was built with KeepUnrecoverable. Lost
	// are the same kind of blocks, deleted anyway because the plan was built
	// with DeleteUnrecoverable. Unrecoverable says why for each.
	Kept          []ulid.ULID
	Lost          []ulid.ULID
	Unrecoverable map[ulid.ULID]string

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
	// Unrecoverable says what to do with a produced block whose sources cannot
	// be brought back. The default refuses to plan.
	Unrecoverable UnrecoverablePolicy
}

// UnrecoverablePolicy is what a rollback does with a block a worker made
// whose sources cannot be restored: one was physically deleted, or carries a
// deletion mark the rollback does not remove, such as retention's.
type UnrecoverablePolicy string

const (
	// RefuseUnrecoverable fails planning and names the blocks.
	RefuseUnrecoverable UnrecoverablePolicy = "refuse"
	// KeepUnrecoverable leaves such blocks in the bucket. They are ordinary,
	// valid blocks; the standalone compactor takes them on as its own. This
	// loses no data, at the price of not being a complete rollback.
	KeepUnrecoverable UnrecoverablePolicy = "keep"
	// DeleteUnrecoverable deletes them anyway, losing the data they hold.
	DeleteUnrecoverable UnrecoverablePolicy = "delete"
)

func (o RollbackOptions) validate() error {
	switch {
	case o.JournalID == "" && !o.AllJournals:
		return errors.New("a journal ID is required, or AllJournals to undo the work of every manager")
	case o.JournalID != "" && o.AllJournals:
		return errors.New("a journal ID and AllJournals are mutually exclusive")
	}
	switch o.Unrecoverable {
	case "", RefuseUnrecoverable, KeepUnrecoverable, DeleteUnrecoverable:
	default:
		return errors.Errorf("unknown policy %q for unrecoverable blocks", o.Unrecoverable)
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
		p, ok := ProvenanceOf(m)
		if !ok || !opts.matches(p.JournalID) {
			continue
		}
		produced[id] = p
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

	// A mark the rollback may remove: the manager's own, or the one the
	// standalone compactor's garbage collection writes on a block it finds
	// covered by another. The latter lands on a source when the manager was
	// interrupted between a result becoming visible and marking its sources;
	// removing it is safe, because if the source really is covered by a block
	// that stays, garbage collection marks it again.
	removable := func(id ulid.ULID) bool {
		details, marked := marks[id]
		if !marked {
			return true
		}
		if details == outdatedBlockDetails {
			return true
		}
		markJournal, _, ok := ParseDeletionDetails(details)
		return ok && opts.matches(markJournal)
	}

	// Deleting a produced block is only safe when every block its task
	// consumed can be brought back: still in the bucket, and either unmarked
	// or marked by something the rollback removes. A source that is itself a
	// produced block is fine as long as that block is deleted too, since its
	// own sources are then checked in turn; once it is kept instead, it has to
	// pass the same test as any other source, so this runs until nothing
	// changes any more.
	r.Unrecoverable = map[ulid.ULID]string{}
	deletable := maps.Clone(produced)
	for changed := true; changed; {
		changed = false
		for id, p := range deletable {
			reason := ""
			if len(p.Sources) == 0 {
				reason = "its provenance names no sources"
			}
			for _, raw := range p.Sources {
				src, err := ulid.Parse(raw)
				if err != nil {
					reason = fmt.Sprintf("its provenance names an invalid source %q", raw)
					break
				}
				if _, ok := deletable[src]; ok {
					continue
				}
				if _, ok := metas[src]; !ok {
					reason = fmt.Sprintf("source %s is no longer in the bucket", src)
					break
				}
				if !removable(src) {
					reason = fmt.Sprintf("source %s is marked for deletion by something else: %q", src, marks[src])
					break
				}
			}
			if reason == "" {
				continue
			}
			r.Unrecoverable[id] = reason
			if opts.Unrecoverable != DeleteUnrecoverable {
				delete(deletable, id)
				changed = true
			}
		}
	}
	for id, reason := range r.Unrecoverable {
		if _, ok := deletable[id]; ok {
			r.Lost = append(r.Lost, id)
			level.Warn(logger).Log("msg", "deleting a block whose sources cannot be restored, as allowed; its data is lost", "block", id, "reason", reason)
		} else {
			r.Kept = append(r.Kept, id)
		}
	}
	slices.SortFunc(r.Kept, func(a, b ulid.ULID) int { return a.Compare(b) })
	slices.SortFunc(r.Lost, func(a, b ulid.ULID) int { return a.Compare(b) })
	if len(r.Kept) > 0 && opts.Unrecoverable != KeepUnrecoverable {
		return nil, errors.Errorf("%d block(s) made by workers cannot be rolled back because what they replaced cannot be brought back, "+
			"so refusing: %s. Keep them in place with the keep policy (no data is lost, the standalone compactor takes them on), "+
			"or delete them anyway with the delete policy (their data is lost)", len(r.Kept), describeUnrecoverable(r.Kept, r.Unrecoverable))
	}
	for _, id := range r.Kept {
		level.Warn(logger).Log("msg", "keeping a block whose sources cannot be restored, as allowed", "block", id, "reason", r.Unrecoverable[id])
	}

	restore := map[ulid.ULID]struct{}{}
	for id := range metas {
		if _, ok := deletable[id]; ok {
			// A produced block that was later consumed by another produced block
			// is marked by the manager too, but it goes, it is not restored.
			continue
		}
		details, marked := marks[id]
		if !marked {
			continue
		}
		markJournal, _, ok := ParseDeletionDetails(details)
		if ok && opts.matches(markJournal) {
			restore[id] = struct{}{}
		}
	}
	for id, p := range deletable {
		r.Produced = append(r.Produced, id)
		for _, raw := range p.Sources {
			src, err := ulid.Parse(raw)
			if err != nil {
				// Only possible under DeleteUnrecoverable, and recorded above.
				continue
			}
			if _, ok := deletable[src]; ok {
				continue
			}
			if _, marked := marks[src]; marked && removable(src) {
				restore[src] = struct{}{}
			}
		}
	}
	r.Restore = slices.Collect(maps.Keys(restore))

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

// outdatedBlockDetails is what the compactor's garbage collection writes on
// a block whose sources are all covered by another block, see
// Syncer.GarbageCollect.
const outdatedBlockDetails = "outdated block"

func describeUnrecoverable(ids []ulid.ULID, reasons map[ulid.ULID]string) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%s (%s)", id, reasons[id]))
	}
	return strings.Join(parts, ", ")
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

// Apply carries the rollback out: deletes the produced blocks, then removes
// the manager's deletion marks from the blocks they replaced.
func (r *Rollback) Apply(ctx context.Context, logger log.Logger, bkt objstore.Bucket) error {
	for _, id := range r.Produced {
		level.Info(logger).Log("msg", "deleting block produced by the distributed compactor", "block", id)
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			return errors.Wrapf(err, "delete produced block %s", id)
		}
	}

	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "thanos_compact_rollback_marks_removed_total"})
	for _, id := range r.Restore {
		level.Info(logger).Log("msg", "restoring block marked for deletion by the distributed compactor", "block", id)
		if err := block.RemoveMark(ctx, logger, bkt, id, counter, metadata.DeletionMarkFilename); err != nil {
			return errors.Wrapf(err, "remove deletion mark of %s", id)
		}
	}
	return nil
}

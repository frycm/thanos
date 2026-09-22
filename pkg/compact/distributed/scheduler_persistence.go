// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/compact"
)

// persistLocked snapshots state while holding mtx, then releases it for all
// bucket I/O and while waiting for other writes. It returns with mtx held.
// Callers must recheck their task/lease identity before rolling back a failure:
// another operation can advance the task while this snapshot is being written.
func (s *Scheduler) persistLocked(ctx context.Context) error {
	snap, err := s.snapshotLocked()
	if err != nil {
		return err
	}
	s.mtx.Unlock()
	err = s.persistSnapshot(ctx, snap)
	s.mtx.Lock()
	if err == nil {
		s.lastPersist = time.Now()
	}
	return err
}

// journalSnapshot is immutable once the state lock has been released.
type journalSnapshot struct {
	seq  uint64
	body []byte
}

func (s *Scheduler) snapshotLocked() (journalSnapshot, error) {
	s.persistSeq++
	s.journal.Version = JournalVersion
	s.journal.UpdatedAt = time.Now()
	body, err := json.Marshal(s.journal)
	if err != nil {
		return journalSnapshot{}, errors.Wrap(err, "marshal journal")
	}
	return journalSnapshot{seq: s.persistSeq, body: body}, nil
}

// persistSnapshot never takes mtx. A newer persisted snapshot already includes
// the state represented by an older one, so out-of-order arrivals are skipped.
func (s *Scheduler) persistSnapshot(ctx context.Context, snap journalSnapshot) error {
	s.persistMtx.Lock()
	defer s.persistMtx.Unlock()

	if snap.seq <= s.persistedSeq {
		return nil
	}
	// A manager that was stopped writes nothing more: its successor may
	// already own the journal, and a store that ignores the context would
	// let the write through.
	if err := ctx.Err(); err != nil {
		return err
	}

	// The generation and owner are fixed at construction, so reading them
	// without the state lock is safe.
	current, err := ReadJournal(ctx, s.bkt, s.conf.JournalID)
	if err != nil {
		return s.recordPersistFailure(errors.Wrap(err, "verify journal ownership"))
	}
	if current != nil && (current.Generation != s.journal.Generation || current.Owner != s.ownerID) {
		// Another manager owns this journal now. The owner ID matters as much
		// as the generation: two managers starting at the same time both bump
		// the same generation, so only the owner tells them apart. Two managers
		// writing the same journal is a misconfiguration this design cannot
		// recover from, so stop.
		return compact.NewHaltError(errors.Errorf(
			"journal %s was taken over by another manager (generation %d owner %q, ours is %d owner %q); "+
				"only one compactor manager may run per shard",
			s.conf.JournalID, current.Generation, current.Owner, s.journal.Generation, s.ownerID))
	}

	if err := s.bkt.Upload(ctx, JournalPath(s.conf.JournalID), bytes.NewReader(snap.body)); err != nil {
		return s.recordPersistFailure(errors.Wrap(err, "write journal"))
	}

	s.persistedSeq = snap.seq
	s.journalUnavailableSince = time.Time{}
	s.m.journalWrites.Inc()
	return nil
}

func (s *Scheduler) recordPersistFailure(err error) error {
	s.m.journalWriteFails.Inc()
	if s.journalUnavailableSince.IsZero() {
		s.journalUnavailableSince = time.Now()
	}
	if time.Since(s.journalUnavailableSince) > s.conf.JournalUnavailableTimeout {
		return compact.NewHaltError(errors.Wrapf(err, "journal has been unwritable for %s", s.conf.JournalUnavailableTimeout))
	}
	return err
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"encoding/json"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestProvenanceStampAndReadBack(t *testing.T) {
	id := ulid.MustNew(7, nil)
	p := Provenance{TaskID: "t1", TaskType: TaskCompaction, WorkerID: "w1", JournalID: "shard-a", Generation: 3}.For(id, []string{"s1", "s2"})

	t.Run("onto empty extensions", func(t *testing.T) {
		ext, err := p.Stamp(nil)
		testutil.Ok(t, err)

		m := &metadata.Meta{}
		m.ULID = id
		m.Thanos.Extensions = ext
		got, ok := ProvenanceOf(m)
		testutil.Equals(t, true, ok)
		testutil.Equals(t, p, got)
	})

	t.Run("keeps what was already there", func(t *testing.T) {
		ext, err := p.Stamp(map[string]any{"downstream": "value"})
		testutil.Ok(t, err)
		testutil.Equals(t, "value", ext.(map[string]any)["downstream"])

		m := &metadata.Meta{}
		m.ULID = id
		m.Thanos.Extensions = ext
		_, ok := ProvenanceOf(m)
		testutil.Equals(t, true, ok)
	})

	t.Run("names one block only", func(t *testing.T) {
		// Extensions are inherited by compaction and copied by downsampling,
		// so a stamp written for one block turns up on blocks made from it.
		// Those were not produced by the task, and a rollback must not find
		// them.
		ext, err := p.Stamp(nil)
		testutil.Ok(t, err)

		m := &metadata.Meta{}
		m.ULID = ulid.MustNew(8, nil)
		m.Thanos.Extensions = ext
		_, ok := ProvenanceOf(m)
		testutil.Equals(t, false, ok)

		// A stamp from before block IDs were recorded is not trusted either.
		old, err := Provenance{TaskID: "t1", TaskType: TaskCompaction, JournalID: "shard-a"}.Stamp(nil)
		testutil.Ok(t, err)
		m.Thanos.Extensions = old
		_, ok = ProvenanceOf(m)
		testutil.Equals(t, false, ok)
	})

	t.Run("survives meta.json", func(t *testing.T) {
		// What matters is reading it back from a block in the bucket, where
		// extensions come back as a generic map.
		ext, err := p.Stamp(nil)
		testutil.Ok(t, err)
		m := metadata.Meta{}
		m.ULID = id
		m.Thanos.Extensions = ext

		raw, err := json.Marshal(m)
		testutil.Ok(t, err)
		var back metadata.Meta
		testutil.Ok(t, json.Unmarshal(raw, &back))

		got, ok := ProvenanceOf(&back)
		testutil.Equals(t, true, ok)
		testutil.Equals(t, p, got)
	})

	t.Run("refuses extensions it cannot merge into", func(t *testing.T) {
		_, err := p.Stamp([]string{"not", "a", "map"})
		testutil.NotOk(t, err)
	})

	t.Run("absent on ordinary blocks", func(t *testing.T) {
		_, ok := ProvenanceOf(&metadata.Meta{})
		testutil.Equals(t, false, ok)

		m := &metadata.Meta{}
		m.Thanos.Extensions = map[string]any{"something": "else"}
		_, ok = ProvenanceOf(m)
		testutil.Equals(t, false, ok)
	})
}

func TestDeletionDetailsRoundTrip(t *testing.T) {
	details := DeletionDetails("shard-a", "01ARZ3NDEKTSV4RRFFQ69G5FAV")

	journal, task, ok := ParseDeletionDetails(details)
	testutil.Equals(t, true, ok)
	testutil.Equals(t, "shard-a", journal)
	testutil.Equals(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV", task)

	// Marks written by anything else are not ours.
	for _, other := range []string{
		"source of compacted block",
		"retention",
		"manual cleanup by operator",
		"",
	} {
		_, _, ok := ParseDeletionDetails(other)
		testutil.Equals(t, false, ok)
	}
}

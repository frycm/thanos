// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// TestVerifyDownsampledBlock asserts the manager only accepts a reported block
// as a downsample of the candidate when it actually is one: right resolution,
// same labels, same time range, built from the same sources. Downsampling
// deletes nothing, so a wrong block cannot lose data, but accepting one would
// leave the candidate silently never downsampled.
func TestVerifyDownsampledBlock(t *testing.T) {
	src1, src2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)

	sourceMeta := &metadata.Meta{}
	sourceMeta.ULID = ulid.MustNew(3, nil)
	sourceMeta.MinTime, sourceMeta.MaxTime = 0, downsample.ResLevel1DownsampleRange
	sourceMeta.Compaction.Sources = []ulid.ULID{src1, src2}
	sourceMeta.Thanos.Labels = map[string]string{"ext": "1"}
	sourceMeta.Thanos.Downsample.Resolution = downsample.ResLevel0

	candidate := downsample.Candidate{Meta: sourceMeta, TargetResolution: downsample.ResLevel1}
	want := Provenance{TaskID: "d1", TaskType: TaskDownsample, JournalID: "shard-a", Generation: 2}

	stamp := func(t *testing.T, m *metadata.Meta, p Provenance) {
		ext, err := p.For(m.ULID, []string{sourceMeta.ULID.String()}).Stamp(nil)
		testutil.Ok(t, err)
		m.Thanos.Extensions = ext
	}
	valid := func(t *testing.T) metadata.Meta {
		m := metadata.Meta{}
		m.ULID = ulid.MustNew(9, nil)
		m.MinTime, m.MaxTime = sourceMeta.MinTime, sourceMeta.MaxTime
		m.Compaction.Sources = []ulid.ULID{src1, src2}
		m.Thanos.Labels = map[string]string{"ext": "1"}
		m.Thanos.Downsample.Resolution = downsample.ResLevel1
		stamp(t, &m, want)
		return m
	}

	for _, tcase := range []struct {
		name    string
		mutate  func(t *testing.T, m *metadata.Meta)
		wantErr bool
	}{
		{
			name:   "valid downsample",
			mutate: func(*testing.T, *metadata.Meta) {},
		},
		{
			name:    "wrong resolution",
			mutate:  func(_ *testing.T, m *metadata.Meta) { m.Thanos.Downsample.Resolution = downsample.ResLevel2 },
			wantErr: true,
		},
		{
			name:    "wrong labels",
			mutate:  func(_ *testing.T, m *metadata.Meta) { m.Thanos.Labels = map[string]string{"ext": "other"} },
			wantErr: true,
		},
		{
			name:    "wrong time range",
			mutate:  func(_ *testing.T, m *metadata.Meta) { m.MaxTime++ },
			wantErr: true,
		},
		{
			name:    "wrong sources",
			mutate:  func(_ *testing.T, m *metadata.Meta) { m.Compaction.Sources = []ulid.ULID{src1, ulid.MustNew(42, nil)} },
			wantErr: true,
		},
		{
			name:    "fewer sources",
			mutate:  func(_ *testing.T, m *metadata.Meta) { m.Compaction.Sources = []ulid.ULID{src1} },
			wantErr: true,
		},
		// The block has to record exactly the task it is reported for: the
		// rollback finds downsampled blocks by that record.
		{
			name:    "no provenance",
			mutate:  func(_ *testing.T, m *metadata.Meta) { m.Thanos.Extensions = nil },
			wantErr: true,
		},
		{
			name: "provenance of another task",
			mutate: func(t *testing.T, m *metadata.Meta) {
				stamp(t, m, Provenance{TaskID: "d2", TaskType: TaskDownsample, JournalID: "shard-a", Generation: 2})
			},
			wantErr: true,
		},
		{
			name: "provenance of a compaction task",
			mutate: func(t *testing.T, m *metadata.Meta) {
				stamp(t, m, Provenance{TaskID: "d1", TaskType: TaskCompaction, JournalID: "shard-a", Generation: 2})
			},
			wantErr: true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			m := valid(t)
			tcase.mutate(t, &m)
			err := verifyDownsampledBlock(&m, candidate, want)
			if tcase.wantErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
		})
	}
}

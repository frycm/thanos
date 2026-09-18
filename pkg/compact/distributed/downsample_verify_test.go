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

	valid := func() metadata.Meta {
		m := metadata.Meta{}
		m.ULID = ulid.MustNew(9, nil)
		m.MinTime, m.MaxTime = sourceMeta.MinTime, sourceMeta.MaxTime
		m.Compaction.Sources = []ulid.ULID{src1, src2}
		m.Thanos.Labels = map[string]string{"ext": "1"}
		m.Thanos.Downsample.Resolution = downsample.ResLevel1
		ext, err := want.For(m.ULID, []string{sourceMeta.ULID.String()}).Stamp(nil)
		testutil.Ok(t, err)
		m.Thanos.Extensions = ext
		return m
	}

	ok := valid()
	testutil.Ok(t, verifyDownsampledBlock(&ok, candidate, want))

	wrongRes := valid()
	wrongRes.Thanos.Downsample.Resolution = downsample.ResLevel2
	testutil.NotOk(t, verifyDownsampledBlock(&wrongRes, candidate, want))

	wrongLabels := valid()
	wrongLabels.Thanos.Labels = map[string]string{"ext": "other"}
	testutil.NotOk(t, verifyDownsampledBlock(&wrongLabels, candidate, want))

	wrongSpan := valid()
	wrongSpan.MaxTime++
	testutil.NotOk(t, verifyDownsampledBlock(&wrongSpan, candidate, want))

	wrongSources := valid()
	wrongSources.Compaction.Sources = []ulid.ULID{src1, ulid.MustNew(42, nil)}
	testutil.NotOk(t, verifyDownsampledBlock(&wrongSources, candidate, want))

	fewerSources := valid()
	fewerSources.Compaction.Sources = []ulid.ULID{src1}
	testutil.NotOk(t, verifyDownsampledBlock(&fewerSources, candidate, want))

	// The block has to record exactly the task it is reported for: the
	// rollback finds downsampled blocks by that record.
	noProvenance := valid()
	noProvenance.Thanos.Extensions = nil
	testutil.NotOk(t, verifyDownsampledBlock(&noProvenance, candidate, want))

	otherTask := valid()
	ext, err := Provenance{TaskID: "d2", TaskType: TaskDownsample, JournalID: "shard-a", Generation: 2}.For(ulid.MustNew(9, nil), []string{sourceMeta.ULID.String()}).Stamp(nil)
	testutil.Ok(t, err)
	otherTask.Thanos.Extensions = ext
	testutil.NotOk(t, verifyDownsampledBlock(&otherTask, candidate, want))

	compactionTask := valid()
	ext, err = Provenance{TaskID: "d1", TaskType: TaskCompaction, JournalID: "shard-a", Generation: 2}.For(ulid.MustNew(9, nil), []string{sourceMeta.ULID.String()}).Stamp(nil)
	testutil.Ok(t, err)
	compactionTask.Thanos.Extensions = ext
	testutil.NotOk(t, verifyDownsampledBlock(&compactionTask, candidate, want))
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestFormatShardID(t *testing.T) {
	testutil.Equals(t, "1_of_1", FormatShardID(1, 1))
	testutil.Equals(t, "3_of_4", FormatShardID(3, 4))
	testutil.Equals(t, "12_of_16", FormatShardID(12, 16))
}

func TestParseShardID(t *testing.T) {
	for _, tc := range []struct {
		value        string
		index, count int
		err          bool
	}{
		{value: "1_of_1", index: 1, count: 1},
		{value: "1_of_2", index: 1, count: 2},
		{value: "2_of_2", index: 2, count: 2},
		{value: "3_of_3", index: 3, count: 3},
		{value: "16_of_16", index: 16, count: 16},
		{value: "7_of_100", index: 7, count: 100},

		{value: "", err: true},
		{value: "1", err: true},
		{value: "1_of_", err: true},
		{value: "_of_2", err: true},
		{value: "1of2", err: true},
		{value: "1_OF_2", err: true},
		{value: "1-of-2", err: true},
		{value: "0_of_2", err: true},
		{value: "3_of_2", err: true},
		{value: "1_of_0", err: true},
		{value: "0_of_0", err: true},
		{value: "-1_of_2", err: true},
		{value: "+1_of_2", err: true},
		{value: "1_of_+2", err: true},
		{value: "01_of_2", err: true},
		{value: "1_of_02", err: true},
		{value: " 1_of_2", err: true},
		{value: "1_of_2 ", err: true},
		{value: "1_of_2_of_3", err: true},
		{value: "a_of_2", err: true},
		{value: "1_of_b", err: true},
		{value: "1.0_of_2", err: true},
		{value: "1_of_99999999999999999999999", err: true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			index, count, err := ParseShardID(tc.value)
			if tc.err {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tc.index, index)
			testutil.Equals(t, tc.count, count)
			testutil.Equals(t, tc.value, FormatShardID(index, count))
		})
	}
}

func TestParseShardID_RoundTrip(t *testing.T) {
	for count := 1; count <= 64; count++ {
		for index := 1; index <= count; index++ {
			i, c, err := ParseShardID(FormatShardID(index, count))
			testutil.Ok(t, err)
			testutil.Equals(t, index, i)
			testutil.Equals(t, count, c)
		}
	}
}

func TestThanos_StreamLabels(t *testing.T) {
	for _, tc := range []struct {
		name     string
		labels   map[string]string
		expected map[string]string
	}{
		{
			name:     "no labels",
			labels:   nil,
			expected: nil,
		},
		{
			name:     "no shard label",
			labels:   map[string]string{"cluster": "a"},
			expected: map[string]string{"cluster": "a"},
		},
		{
			name:     "valid shard label",
			labels:   map[string]string{"cluster": "a", CompactorShardIDLabel: "2_of_4"},
			expected: map[string]string{"cluster": "a"},
		},
		{
			name:     "only a valid shard label",
			labels:   map[string]string{CompactorShardIDLabel: "1_of_1"},
			expected: map[string]string{},
		},
		{
			name:     "invalid shard label is kept",
			labels:   map[string]string{"cluster": "a", CompactorShardIDLabel: "5_of_4"},
			expected: map[string]string{"cluster": "a", CompactorShardIDLabel: "5_of_4"},
		},
		{
			name:     "empty shard label is kept",
			labels:   map[string]string{"cluster": "a", CompactorShardIDLabel: ""},
			expected: map[string]string{"cluster": "a", CompactorShardIDLabel: ""},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Thanos{Labels: tc.labels}
			testutil.Equals(t, tc.expected, m.StreamLabels())
			// The block's own labels are left alone.
			_, hadShardID := tc.labels[CompactorShardIDLabel]
			_, hasShardID := m.Labels[CompactorShardIDLabel]
			testutil.Equals(t, hadShardID, hasShardID)
		})
	}
}

func TestThanos_StreamGroupKey(t *testing.T) {
	unsplit := Thanos{Labels: map[string]string{"cluster": "a"}, Downsample: ThanosDownsample{Resolution: 300000}}
	shard := Thanos{Labels: map[string]string{"cluster": "a", CompactorShardIDLabel: "1_of_2"}, Downsample: ThanosDownsample{Resolution: 300000}}
	otherShard := Thanos{Labels: map[string]string{"cluster": "a", CompactorShardIDLabel: "2_of_2"}, Downsample: ThanosDownsample{Resolution: 300000}}
	invalidShard := Thanos{Labels: map[string]string{"cluster": "a", CompactorShardIDLabel: "0_of_2"}, Downsample: ThanosDownsample{Resolution: 300000}}
	otherResolution := Thanos{Labels: map[string]string{"cluster": "a", CompactorShardIDLabel: "1_of_2"}}
	otherStream := Thanos{Labels: map[string]string{"cluster": "b", CompactorShardIDLabel: "1_of_2"}, Downsample: ThanosDownsample{Resolution: 300000}}

	// Without a valid shard label, the stream group key is the group key.
	testutil.Equals(t, unsplit.GroupKey(), unsplit.StreamGroupKey())
	testutil.Equals(t, invalidShard.GroupKey(), invalidShard.StreamGroupKey())

	testutil.Equals(t, unsplit.StreamGroupKey(), shard.StreamGroupKey())
	testutil.Equals(t, unsplit.StreamGroupKey(), otherShard.StreamGroupKey())
	testutil.Assert(t, shard.GroupKey() != otherShard.GroupKey())
	testutil.Assert(t, unsplit.StreamGroupKey() != invalidShard.StreamGroupKey())
	testutil.Assert(t, unsplit.StreamGroupKey() != otherResolution.StreamGroupKey())
	testutil.Assert(t, unsplit.StreamGroupKey() != otherStream.StreamGroupKey())
}

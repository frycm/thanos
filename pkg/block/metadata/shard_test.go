// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestShardLabelValue(t *testing.T) {
	for _, tc := range []struct {
		index, count uint64
		want         string
	}{
		{0, 1, "1_of_1"},
		{0, 4, "1_of_4"},
		{3, 4, "4_of_4"},
		{11, 16, "12_of_16"},
	} {
		got := FormatShardLabelValue(tc.index, tc.count)
		testutil.Equals(t, tc.want, got)
		index, count, err := ParseShardLabelValue(got)
		testutil.Ok(t, err)
		testutil.Equals(t, tc.index, index)
		testutil.Equals(t, tc.count, count)
	}
	for _, bad := range []string{"", "1", "1_of_", "_of_4", "0_of_4", "5_of_4", "1_of_0", "a_of_b", "1-of-4"} {
		_, _, err := ParseShardLabelValue(bad)
		testutil.NotOk(t, err, "value %q must not parse", bad)
	}
}

func TestShard(t *testing.T) {
	_, _, ok, err := Shard(map[string]string{"cluster": "eu1"})
	testutil.Ok(t, err)
	testutil.Equals(t, false, ok)

	index, count, ok, err := Shard(map[string]string{"cluster": "eu1", CompactorShardLabel: "3_of_8"})
	testutil.Ok(t, err)
	testutil.Equals(t, true, ok)
	testutil.Equals(t, uint64(2), index)
	testutil.Equals(t, uint64(8), count)

	_, _, _, err = Shard(map[string]string{CompactorShardLabel: "nonsense"})
	testutil.NotOk(t, err)
}

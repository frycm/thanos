// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestShardLabelValue(t *testing.T) {
	for _, tc := range []struct {
		name         string
		index, count uint64
		want         string
	}{
		{name: "single shard", index: 0, count: 1, want: "1_of_1"},
		{name: "first of four", index: 0, count: 4, want: "1_of_4"},
		{name: "last of four", index: 3, count: 4, want: "4_of_4"},
		{name: "two digits", index: 11, count: 16, want: "12_of_16"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatShardLabelValue(tc.index, tc.count)
			testutil.Equals(t, tc.want, got)
			index, count, err := ParseShardLabelValue(got)
			testutil.Ok(t, err)
			testutil.Equals(t, tc.index, index)
			testutil.Equals(t, tc.count, count)
		})
	}
	for _, tc := range []struct {
		name  string
		value string
	}{
		{name: "rejects empty", value: ""},
		{name: "rejects number only", value: "1"},
		{name: "rejects missing count", value: "1_of_"},
		{name: "rejects missing index", value: "_of_4"},
		{name: "rejects zero index", value: "0_of_4"},
		{name: "rejects index above count", value: "5_of_4"},
		{name: "rejects zero count", value: "1_of_0"},
		{name: "rejects non-numbers", value: "a_of_b"},
		{name: "rejects wrong separator", value: "1-of-4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ParseShardLabelValue(tc.value)
			testutil.NotOk(t, err, "value %q must not parse", tc.value)
		})
	}
}

func TestShard(t *testing.T) {
	for _, tc := range []struct {
		name string

		labels map[string]string

		wantIndex, wantCount uint64
		wantOK               bool
		wantErr              bool
	}{
		{
			name:   "no shard label",
			labels: map[string]string{"cluster": "eu1"},
		},
		{
			name:      "shard label",
			labels:    map[string]string{"cluster": "eu1", CompactorShardLabel: "3_of_8"},
			wantIndex: 2,
			wantCount: 8,
			wantOK:    true,
		},
		{
			name:    "shard label that does not parse",
			labels:  map[string]string{CompactorShardLabel: "nonsense"},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index, count, ok, err := Shard(tc.labels)
			if tc.wantErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			testutil.Equals(t, tc.wantIndex, index)
			testutil.Equals(t, tc.wantCount, count)
		})
	}
}

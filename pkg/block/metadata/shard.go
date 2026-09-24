// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// CompactorShardLabel is the external label the compactor puts on a block that
// holds one shard of a compaction split by series. Its value is "i_of_M",
// 1-based: shard i of M holds every series whose stable label hash is
// congruent to i-1 modulo M. A block without the label is unsplit.
//
// It is an external label so that the grouper, the planner, the metadata
// deduplication filter and the store gateway treat every shard as its own
// block stream without knowing about shards; the store gateway strips it from
// what it serves.
const CompactorShardLabel = "__compactor_shard__"

// FormatShardLabelValue renders the label value for the 0-based shard index
// of count.
func FormatShardLabelValue(index, count uint64) string {
	return fmt.Sprintf("%d_of_%d", index+1, count)
}

// ParseShardLabelValue returns the 0-based shard index and the count.
func ParseShardLabelValue(v string) (index, count uint64, err error) {
	i, m, ok := strings.Cut(v, "_of_")
	if !ok {
		return 0, 0, errors.Errorf("shard label value %q is not of the form i_of_M", v)
	}
	index, err = strconv.ParseUint(i, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "shard label value %q", v)
	}
	count, err = strconv.ParseUint(m, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "shard label value %q", v)
	}
	if index == 0 || count == 0 || index > count {
		return 0, 0, errors.Errorf("shard label value %q is out of range", v)
	}
	return index - 1, count, nil
}

// Shard returns the 0-based shard index and count recorded in the labels, or
// ok false for an unsplit block.
func Shard(lbls map[string]string) (index, count uint64, ok bool, err error) {
	v, found := lbls[CompactorShardLabel]
	if !found {
		return 0, 0, false, nil
	}
	index, count, err = ParseShardLabelValue(v)
	if err != nil {
		return 0, 0, false, err
	}
	return index, count, true, nil
}

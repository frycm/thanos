// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

// CompactorShardIDLabel is the external label of a block holding only one shard of the series of the blocks it
// was made from (its Compaction.Sources). The value has the form "<index>_of_<count>" (see FormatShardID): the
// block holds the series of its sources that belong to the index-th of count shards, index being 1-based.
// The name and the format are the ones used by Grafana Mimir, so that tooling understands either.
const CompactorShardIDLabel = "__compactor_shard_id__"

const shardIDSeparator = "_of_"

// FormatShardID returns the CompactorShardIDLabel value of the index-th (1-based) of count shards.
func FormatShardID(index, count int) string {
	return fmt.Sprintf("%d%s%d", index, shardIDSeparator, count)
}

// ParseShardID parses a CompactorShardIDLabel value. Only values FormatShardID can produce are accepted: count is at
// least 1, index is in [1, count], and both are written in decimal without sign or leading zeros, so each shard has
// exactly one valid value.
func ParseShardID(v string) (index, count int, err error) {
	indexStr, countStr, ok := strings.Cut(v, shardIDSeparator)
	if !ok {
		return 0, 0, errors.Errorf("invalid shard ID %q: expected <index>%s<count>", v, shardIDSeparator)
	}
	if index, err = parseShardIDNumber(indexStr); err != nil {
		return 0, 0, errors.Wrapf(err, "invalid shard ID %q: index", v)
	}
	if count, err = parseShardIDNumber(countStr); err != nil {
		return 0, 0, errors.Wrapf(err, "invalid shard ID %q: count", v)
	}
	if index > count {
		return 0, 0, errors.Errorf("invalid shard ID %q: index out of range [1, %d]", v, count)
	}
	return index, count, nil
}

// parseShardIDNumber parses a positive decimal number without sign or leading zeros.
func parseShardIDNumber(s string) (int, error) {
	if s == "" {
		return 0, errors.New("empty number")
	}
	if s[0] == '0' {
		return 0, errors.Errorf("%q is not a positive number without leading zeros", s)
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.Errorf("%q is not a decimal number", s)
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ShardID returns the value of the block's CompactorShardIDLabel and whether the block has that label.
func (m *Thanos) ShardID() (string, bool) {
	v, ok := m.Labels[CompactorShardIDLabel]
	return v, ok
}

// StreamLabels returns the external labels of the stream the block belongs to: its labels without a valid
// CompactorShardIDLabel, so that every shard of a split shares them with the blocks the split was made from.
// A shard label that does not parse is kept, so such a block only ever shares a stream with blocks carrying the
// very same labels. When there is nothing to remove, the block's own label map is returned; it must not be modified.
func (m *Thanos) StreamLabels() map[string]string {
	v, ok := m.ShardID()
	if !ok {
		return m.Labels
	}
	if _, _, err := ParseShardID(v); err != nil {
		return m.Labels
	}
	lset := make(map[string]string, len(m.Labels)-1)
	for k, v := range m.Labels {
		if k != CompactorShardIDLabel {
			lset[k] = v
		}
	}
	return lset
}

// StreamGroupKey is like GroupKey but over StreamLabels: the shards of a split share it with the blocks they were
// made from. For a block without a valid CompactorShardIDLabel it equals GroupKey.
func (m *Thanos) StreamGroupKey() string {
	return fmt.Sprintf("%d@%v", m.Downsample.Resolution, labels.FromMap(m.StreamLabels()).Hash())
}

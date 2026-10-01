// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"fmt"
	"slices"
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

const (
	// CompactorSplitExtensionKey is the key, in the Thanos extensions of a block's meta.json, of the SplitScheme the
	// block's shard was computed with. The compactor writes it into every block it splits, and keeps it when it
	// compacts shard blocks further, so that a stream is never split again with a different scheme in a time range
	// that already holds shard blocks.
	CompactorSplitExtensionKey = "compactor_split"

	// SplitHashStable is the SplitScheme.Hash of shards computed with labels.StableHash of the series labels, less
	// the SplitScheme.IgnoreLabels: series s belongs to shard (hash(s) mod count) + 1.
	SplitHashStable = "stable_hash"
)

// SplitScheme records how the series of a split were distributed among its shards. Two shard blocks only hold
// disjoint series for the same index if they were computed with equal schemes.
type SplitScheme struct {
	// Hash names the hash function; only SplitHashStable is known.
	Hash string `json:"hash"`
	// Shards is the number of shards, the count of the blocks' CompactorShardIDLabel.
	Shards int `json:"shards"`
	// IgnoreLabels are the series labels left out of the hash, sorted and without repetition.
	IgnoreLabels []string `json:"ignore_labels,omitempty"`
}

// Equal tells whether both schemes distribute series the same way.
func (s SplitScheme) Equal(o SplitScheme) bool {
	return s.Hash == o.Hash && s.Shards == o.Shards && slices.Equal(s.IgnoreLabels, o.IgnoreLabels)
}

func (s SplitScheme) String() string {
	return fmt.Sprintf("%s over %d shards ignoring labels %v", s.Hash, s.Shards, s.IgnoreLabels)
}

// SplitScheme returns the scheme recorded in the block's extensions under CompactorSplitExtensionKey, or nil when
// there is none. An error means a record exists but cannot be read.
func (m *Thanos) SplitScheme() (*SplitScheme, error) {
	ext, ok := m.Extensions.(map[string]any)
	if !ok {
		if m.Extensions == nil {
			return nil, nil
		}
		// Typed extensions, e.g. set in memory before the meta is written: read them through their JSON form.
		var generic map[string]any
		if _, err := ConvertExtensions(m.Extensions, &generic); err != nil {
			return nil, nil
		}
		ext = generic
	}
	v, ok := ext[CompactorSplitExtensionKey]
	if !ok {
		return nil, nil
	}
	var s SplitScheme
	if _, err := ConvertExtensions(v, &s); err != nil {
		return nil, errors.Wrapf(err, "parse %s extension", CompactorSplitExtensionKey)
	}
	return &s, nil
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// compactorShardLabel is the external label the compactor puts on a block that
// holds one shard of a compaction split by series: "i_of_M", 1-based, with M a
// power of two. Shard i of M holds the series whose hash is congruent to i-1
// modulo M, so it holds exactly what shards i and i+M of 2M hold together.
//
// It is an external label, so each shard is a label set of its own. Coverage
// keyed on label sets alone would never let the shards of a downsampled block
// cover the unsplit block it was made from, and every such block would be
// served below the minimum resolution for good.
const compactorShardLabel = "__compactor_shard__"

// shardRef is a shard of a block stream: index of count, 0-based. The whole,
// unsplit stream is 0 of 1.
type shardRef struct{ index, count uint64 }

// streamShard returns the block's stream - its external labels without the
// shard label - and its shard. A block whose shard label does not parse is a
// stream of its own, like any other label set: coverage then falls back to
// the exact label set.
func streamShard(lbls map[string]string) (string, shardRef) {
	v, ok := lbls[compactorShardLabel]
	if !ok {
		return labels.FromMap(lbls).String(), shardRef{index: 0, count: 1}
	}
	s, ok := parseShard(v)
	if !ok {
		return labels.FromMap(lbls).String(), shardRef{index: 0, count: 1}
	}
	rest := maps.Clone(lbls)
	delete(rest, compactorShardLabel)
	return labels.FromMap(rest).String(), s
}

func parseShard(v string) (shardRef, bool) {
	i, m, ok := strings.Cut(v, "_of_")
	if !ok {
		return shardRef{}, false
	}
	index, err := strconv.ParseUint(i, 10, 64)
	if err != nil {
		return shardRef{}, false
	}
	count, err := strconv.ParseUint(m, 10, 64)
	if err != nil || index == 0 || count == 0 || index > count || count&(count-1) != 0 {
		return shardRef{}, false
	}
	return shardRef{index: index - 1, count: count}, true
}

// parent is the coarser shard holding this one, if there is one.
func (s shardRef) parent() (shardRef, bool) {
	if s.count <= 1 {
		return s, false
	}
	half := s.count / 2
	return shardRef{index: s.index % half, count: half}, true
}

// children are the two finer shards that together hold this one.
func (s shardRef) children() [2]shardRef {
	return [2]shardRef{{index: s.index, count: 2 * s.count}, {index: s.index + s.count, count: 2 * s.count}}
}

// holds reports whether every series of o is one of s's.
func (s shardRef) holds(o shardRef) bool {
	return o.count >= s.count && o.count%s.count == 0 && o.index%s.count == s.index
}

// streamCoverage is the coverage one block stream's blocks give at one
// resolution, per shard.
type streamCoverage struct {
	shards map[shardRef]metadata.SourceCoverage
	finest uint64
}

// resolutionCoverage is the coverage the blocks at one resolution give, per
// block stream.
type resolutionCoverage map[string]*streamCoverage

// coverageAtResolution indexes the sources of the blocks at exactly the given
// resolution, per block stream and shard.
func coverageAtResolution(metas map[ulid.ULID]*metadata.Meta, resolution int64) resolutionCoverage {
	c := resolutionCoverage{}
	for _, m := range metas {
		if m.Thanos.Downsample.Resolution != resolution {
			continue
		}
		stream, s := streamShard(m.Thanos.Labels)
		sc := c[stream]
		if sc == nil {
			sc = &streamCoverage{shards: map[shardRef]metadata.SourceCoverage{}}
			c[stream] = sc
		}
		if sc.shards[s] == nil {
			sc.shards[s] = metadata.SourceCoverage{}
		}
		sc.shards[s].Add(m)
		sc.finest = max(sc.finest, s.count)
	}
	return c
}

// covers reports whether the stream's blocks cover the block's sources over
// its whole range for every series the block may hold: through its own
// shard, a coarser shard holding it, or finer shards that together hold all
// of it.
func (c resolutionCoverage) covers(m *metadata.Meta) bool {
	stream, s := streamShard(m.Thanos.Labels)
	sc := c[stream]
	if sc == nil {
		return false
	}
	for a, ok := s, true; ok; a, ok = a.parent() {
		if sc.coveredBy(a, m) {
			return true
		}
	}
	return sc.coveredBelow(s, m)
}

func (sc *streamCoverage) coveredBy(s shardRef, m *metadata.Meta) bool {
	cov, ok := sc.shards[s]
	return ok && cov.Covers(m, m.MinTime, m.MaxTime-1)
}

// coveredBelow reports whether the finer shards of s cover the block, each
// half through its own blocks or, in turn, through its finer shards.
func (sc *streamCoverage) coveredBelow(s shardRef, m *metadata.Meta) bool {
	if s.count*2 > sc.finest {
		return false
	}
	for _, child := range s.children() {
		if !sc.coveredBy(child, m) && !sc.coveredBelow(child, m) {
			return false
		}
	}
	return true
}

// coveringBlocks returns the blocks of the stream that can contribute to
// covering the block - those of its own shard, of coarser shards holding it
// and of finer shards within it - sorted and without duplicates.
func (c resolutionCoverage) coveringBlocks(m *metadata.Meta) []ulid.ULID {
	stream, s := streamShard(m.Thanos.Labels)
	sc := c[stream]
	if sc == nil {
		return nil
	}
	var ids []ulid.ULID
	for shard, cov := range sc.shards {
		if shard.holds(s) || s.holds(shard) {
			ids = append(ids, cov.CoveringBlocks(m, m.MinTime, m.MaxTime-1)...)
		}
	}
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return slices.Compact(ids)
}

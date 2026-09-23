// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"sort"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Candidate is a block that should be downsampled, and the resolution it should
// be downsampled to.
type Candidate struct {
	Meta             *metadata.Meta
	TargetResolution int64
}

// Plan returns the blocks that need downsampling, in a deterministic order.
//
// A block is a candidate when no downsampled block covering all of its sources
// exists yet, and it spans enough time to yield roughly two chunks at the target
// resolution. Planning is separated from doing the work so that the same
// decision can drive either downsampling in process or dispatching the work to
// a worker.
func Plan(metas map[ulid.ULID]*metadata.Meta) ([]Candidate, error) {
	// Blocks whose sources are already covered by a downsampled block do not need
	// downsampling again. Coverage is per block stream and shard: the shards of
	// a compaction split by series record the same sources while each holds
	// other series, so a source ULID alone says nothing about which series
	// have been downsampled. A shard's downsampled block covers that shard; an
	// unsplit block is covered by the downsampled blocks of all its shards
	// together, or it would be downsampled again every pass once its own
	// downsampled block was compacted into a split one.
	sources5m := coverage{}
	sources1h := coverage{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case ResLevel0:
			continue
		case ResLevel1:
			sources5m.add(m)
		case ResLevel2:
			sources1h.add(m)
		default:
			return nil, errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	ids := make([]ulid.ULID, 0, len(metas))
	for id := range metas {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Compare(ids[j]) < 0 })

	var candidates []Candidate
	for _, id := range ids {
		m := metas[id]

		switch m.Thanos.Downsample.Resolution {
		case ResLevel2:
			continue

		case ResLevel0:
			if sources5m.covers(m) {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < ResLevel1DownsampleRange {
				continue
			}
			candidates = append(candidates, Candidate{Meta: m, TargetResolution: ResLevel1})

		case ResLevel1:
			if sources1h.covers(m) {
				continue
			}
			if m.MaxTime-m.MinTime < ResLevel2DownsampleRange {
				continue
			}
			candidates = append(candidates, Candidate{Meta: m, TargetResolution: ResLevel2})
		}
	}
	return candidates, nil
}

// coverage records which sources the downsampled blocks of a block stream -
// a set of external labels without the compactor's shard label - account
// for, per shard: the shards a compaction split by series into hold the
// stream's series between them, each only its own part.
type coverage map[uint64]*streamCoverage

type streamCoverage struct {
	shards map[shardRef]map[ulid.ULID]struct{}
	finest uint64
}

// shardRef is a shard of a block stream: index of count, 0-based; the whole,
// unsplit stream is 0 of 1. Shard i of M holds exactly what shards i and i+M
// of 2M hold together.
type shardRef struct{ index, count uint64 }

func (s shardRef) parent() (shardRef, bool) {
	if s.count <= 1 {
		return s, false
	}
	half := s.count / 2
	return shardRef{index: s.index % half, count: half}, true
}

func (s shardRef) children() [2]shardRef {
	return [2]shardRef{{index: s.index, count: 2 * s.count}, {index: s.index + s.count, count: 2 * s.count}}
}

// streamOf returns the block's stream and its shard in it. A block whose
// shard label does not parse is a stream of its own, as any label set.
func streamOf(m *metadata.Meta) (uint64, shardRef) {
	index, count, ok, err := metadata.Shard(m.Thanos.Labels)
	if err != nil || !ok {
		return labels.FromMap(m.Thanos.Labels).Hash(), shardRef{index: 0, count: 1}
	}
	return labels.NewBuilder(labels.FromMap(m.Thanos.Labels)).Del(metadata.CompactorShardLabel).Labels().Hash(), shardRef{index: index, count: count}
}

func (c coverage) add(m *metadata.Meta) {
	stream, s := streamOf(m)
	sc := c[stream]
	if sc == nil {
		sc = &streamCoverage{shards: map[shardRef]map[ulid.ULID]struct{}{}}
		c[stream] = sc
	}
	if sc.shards[s] == nil {
		sc.shards[s] = map[ulid.ULID]struct{}{}
	}
	for _, id := range m.Compaction.Sources {
		sc.shards[s][id] = struct{}{}
	}
	sc.finest = max(sc.finest, s.count)
}

// covers reports whether every source of the block already appears in the
// downsampled blocks of its stream for every series the block may hold:
// through its own shard, a coarser shard holding it, or finer shards that
// together hold all of it. Downsampling one shard never covers another.
func (c coverage) covers(m *metadata.Meta) bool {
	stream, s := streamOf(m)
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
	sources, ok := sc.shards[s]
	if !ok {
		return false
	}
	for _, id := range m.Compaction.Sources {
		if _, ok := sources[id]; !ok {
			return false
		}
	}
	return true
}

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

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Coverage tells whether the data of a block is already present at a lower resolution, so that the block does not
// need to be downsampled (again). Add the downsampled blocks first, then ask with Covers.
//
// A block's data is covered at a resolution when each of its sources is covered there:
//   - by a block of that resolution without a metadata.CompactorShardIDLabel, of any stream, having the source: such a
//     block holds every series of its sources;
//   - for a shard block, by a block of that resolution of the same stream and shard having the source;
//   - for a block without the shard label, by a complete shard family of that resolution and of the same stream: for
//     some count M, for every index i in [1, M], a block labeled "<i>_of_<M>" having the source.
//
// A shard block holds only part of the series of its sources, so it never covers a block of another shard, nor alone
// a block without the shard label. When no block carries the shard label, this is exactly the plain rule: a block is
// covered when each of its sources is a source of some block of the resolution.
type Coverage struct {
	byResolution map[int64]*resolutionCoverage
}

type resolutionCoverage struct {
	// unsplit holds the sources of the blocks without the shard label.
	unsplit map[ulid.ULID]struct{}
	// shards holds, per stream and source, the shard label values of the blocks of the stream having the source.
	shards map[string]map[ulid.ULID]map[string]struct{}
}

// NewCoverage returns an empty Coverage.
func NewCoverage() *Coverage {
	return &Coverage{byResolution: map[int64]*resolutionCoverage{
		ResLevel1: {unsplit: map[ulid.ULID]struct{}{}, shards: map[string]map[ulid.ULID]map[string]struct{}{}},
		ResLevel2: {unsplit: map[ulid.ULID]struct{}{}, shards: map[string]map[ulid.ULID]map[string]struct{}{}},
	}}
}

// Add records the sources of a downsampled block. Raw blocks are ignored.
func (c *Coverage) Add(m *metadata.Meta) error {
	resolution := m.Thanos.Downsample.Resolution
	if resolution == ResLevel0 {
		return nil
	}
	rc, ok := c.byResolution[resolution]
	if !ok {
		return errors.Errorf("unexpected downsampling resolution %d", resolution)
	}

	shardID, ok := m.Thanos.ShardID()
	if !ok {
		for _, id := range m.Compaction.Sources {
			rc.unsplit[id] = struct{}{}
		}
		return nil
	}

	key := streamKey(m)
	sources, ok := rc.shards[key]
	if !ok {
		sources = make(map[ulid.ULID]map[string]struct{}, len(m.Compaction.Sources))
		rc.shards[key] = sources
	}
	for _, id := range m.Compaction.Sources {
		shardIDs, ok := sources[id]
		if !ok {
			shardIDs = make(map[string]struct{}, 1)
			sources[id] = shardIDs
		}
		shardIDs[shardID] = struct{}{}
	}
	return nil
}

// Covers tells whether all data of m is present in the added blocks of the given resolution.
func (c *Coverage) Covers(m *metadata.Meta, resolution int64) bool {
	rc, ok := c.byResolution[resolution]
	if !ok {
		return false
	}

	var (
		streamLooked bool
		sources      map[ulid.ULID]map[string]struct{}
		shardID      string
		sharded      bool
	)
	for _, id := range m.Compaction.Sources {
		if _, ok := rc.unsplit[id]; ok {
			continue
		}
		if len(rc.shards) == 0 {
			return false
		}
		if !streamLooked {
			sources = rc.shards[streamKey(m)]
			shardID, sharded = m.Thanos.ShardID()
			streamLooked = true
		}
		shardIDs := sources[id]
		if sharded {
			if _, ok := shardIDs[shardID]; !ok {
				return false
			}
			continue
		}
		if !completeShardFamily(shardIDs) {
			return false
		}
	}
	return true
}

// streamKey identifies the stream of a block whatever its resolution: its labels without a valid shard label.
// A block whose shard label does not parse keeps it, so it shares its stream only with blocks of identical labels.
func streamKey(m *metadata.Meta) string {
	return labels.FromMap(m.Thanos.StreamLabels()).String()
}

// completeShardFamily tells whether, for some count M, the shard IDs hold "<i>_of_<M>" for every index i in [1, M].
func completeShardFamily(shardIDs map[string]struct{}) bool {
	var perCount map[int]int
	for shardID := range shardIDs {
		_, count, err := metadata.ParseShardID(shardID)
		if err != nil {
			continue
		}
		if perCount == nil {
			perCount = make(map[int]int, 1)
		}
		// Each valid shard has exactly one ID, so distinct IDs of a count are distinct indexes.
		perCount[count]++
		if perCount[count] == count {
			return true
		}
	}
	return false
}

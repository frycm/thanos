// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"sort"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"

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
	// downsampling again.
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case ResLevel0:
			continue
		case ResLevel1:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case ResLevel2:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
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
			if covered(m, sources5m) {
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
			if covered(m, sources1h) {
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

// covered reports whether every source of the block already appears in a
// downsampled block.
func covered(m *metadata.Meta, sources map[ulid.ULID]struct{}) bool {
	for _, id := range m.Compaction.Sources {
		if _, ok := sources[id]; !ok {
			return false
		}
	}
	return true
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"math/rand"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func coverageTestMeta(id uint64, resolution int64, lset map[string]string, sources ...uint64) *metadata.Meta {
	m := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(id, nil)},
		Thanos: metadata.Thanos{
			Labels:     lset,
			Downsample: metadata.ThanosDownsample{Resolution: resolution},
		},
	}
	for _, s := range sources {
		m.Compaction.Sources = append(m.Compaction.Sources, ulid.MustNew(s, nil))
	}
	return m
}

func unsplit(cluster string) map[string]string {
	return map[string]string{"cluster": cluster}
}

func shard(cluster, shardID string) map[string]string {
	return map[string]string{"cluster": cluster, metadata.CompactorShardIDLabel: shardID}
}

func TestCoverage(t *testing.T) {
	for _, tcase := range []struct {
		name       string
		downsample []*metadata.Meta
		block      *metadata.Meta
		resolution int64
		covered    bool
	}{
		{
			name:       "nothing downsampled",
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "unsplit 5m block covers the unsplit raw block",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, unsplit("a"), 1)},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name: "unsplit 5m blocks cover the unsplit raw block together",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, unsplit("a"), 1),
				coverageTestMeta(11, ResLevel1, unsplit("a"), 2),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1, 2),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name:       "unsplit 5m block missing a source",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, unsplit("a"), 1)},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1, 2),
			resolution: ResLevel1,
		},
		{
			name:       "1h block does not cover at 5m",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel2, unsplit("a"), 1)},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "one shard's 5m block does not cover the unsplit raw block",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1)},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "one shard's 5m block does not cover the sibling raw shard",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1)},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "2_of_2"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "one shard's 5m block covers its raw shard",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1)},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "1_of_2"), 1),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name: "one shard's 5m blocks cover its raw shard together",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "1_of_2"), 2),
			},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "1_of_2"), 1, 2),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name:       "a shard of another count does not cover the raw shard",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, shard("a", "1_of_4"), 1)},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "1_of_2"), 1),
			resolution: ResLevel1,
		},
		{
			name: "a complete family of another count does not cover the raw shard",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_1"), 1),
			},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "1_of_2"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "the same shard of another stream does not cover the raw shard",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, shard("b", "1_of_2"), 1)},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "1_of_2"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "unsplit 5m block covers the raw shards",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel1, unsplit("a"), 1, 2)},
			block:      coverageTestMeta(2, ResLevel0, shard("a", "2_of_2"), 1),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name: "partial family does not cover the unsplit raw block",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_4"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "2_of_4"), 1),
				coverageTestMeta(12, ResLevel1, shard("a", "4_of_4"), 1),
				coverageTestMeta(13, ResLevel1, shard("a", "2_of_2"), 1),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
		},
		{
			name: "complete family covers the unsplit raw block",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "2_of_2"), 1, 2),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name: "complete family must cover every source",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "2_of_2"), 1, 2),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1, 2),
			resolution: ResLevel1,
		},
		{
			name: "sources covered by an unsplit block and by complete families",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, unsplit("a"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "1_of_2"), 2),
				coverageTestMeta(12, ResLevel1, shard("a", "2_of_2"), 2),
				coverageTestMeta(13, ResLevel1, shard("a", "1_of_1"), 3),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1, 2, 3),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name: "complete family of another stream does not cover the unsplit raw block",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "2_of_2"), 1),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("b"), 1),
			resolution: ResLevel1,
		},
		{
			name: "complete 1h family covers the unsplit 5m block",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel2, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel2, shard("a", "2_of_2"), 1),
			},
			block:      coverageTestMeta(1, ResLevel1, unsplit("a"), 1),
			resolution: ResLevel2,
			covered:    true,
		},
		{
			name:       "one shard's 1h block does not cover the unsplit 5m block",
			downsample: []*metadata.Meta{coverageTestMeta(10, ResLevel2, shard("a", "1_of_2"), 1)},
			block:      coverageTestMeta(1, ResLevel1, unsplit("a"), 1),
			resolution: ResLevel2,
		},
		{
			name: "complete 5m family does not cover at 1h",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "2_of_2"), 1),
			},
			block:      coverageTestMeta(1, ResLevel1, unsplit("a"), 1),
			resolution: ResLevel2,
		},
		{
			name: "invalid shard label covers only blocks with the same labels",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "x"), 1),
			},
			block:      coverageTestMeta(1, ResLevel0, shard("a", "x"), 1),
			resolution: ResLevel1,
			covered:    true,
		},
		{
			name: "invalid shard label does not cover the unsplit block",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_2"), 1),
				coverageTestMeta(11, ResLevel1, shard("a", "02_of_2"), 1),
				coverageTestMeta(12, ResLevel1, shard("a", "2_of_2x"), 1),
			},
			block:      coverageTestMeta(1, ResLevel0, unsplit("a"), 1),
			resolution: ResLevel1,
		},
		{
			name: "valid shard does not cover a block with an invalid shard label",
			downsample: []*metadata.Meta{
				coverageTestMeta(10, ResLevel1, shard("a", "1_of_1"), 1),
			},
			block:      coverageTestMeta(1, ResLevel0, shard("a", "1_of_0"), 1),
			resolution: ResLevel1,
		},
		{
			name:       "block without sources is covered",
			block:      coverageTestMeta(1, ResLevel0, shard("a", "1_of_2")),
			resolution: ResLevel1,
			covered:    true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			c := NewCoverage()
			for _, m := range tcase.downsample {
				testutil.Ok(t, c.Add(m))
			}
			testutil.Ok(t, c.Add(tcase.block))
			testutil.Equals(t, tcase.covered, c.Covers(tcase.block, tcase.resolution))
		})
	}
}

func TestCoverage_UnexpectedResolution(t *testing.T) {
	c := NewCoverage()
	testutil.NotOk(t, c.Add(coverageTestMeta(1, 1000, unsplit("a"), 1)))
	testutil.Assert(t, !c.Covers(coverageTestMeta(1, ResLevel0, unsplit("a"), 1), 1000))
}

// upstreamCovered is how Thanos v0.41.0 decided whether a block was already downsampled, before shard labels were
// known: when each of its sources is a source of some block of the lower resolution, whatever its labels.
func upstreamCovered(metas []*metadata.Meta, m *metadata.Meta) bool {
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}
	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case ResLevel1:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case ResLevel2:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
		}
	}

	sources := sources5m
	if m.Thanos.Downsample.Resolution == ResLevel1 {
		sources = sources1h
	}
	for _, id := range m.Compaction.Sources {
		if _, ok := sources[id]; !ok {
			return false
		}
	}
	return true
}

func randomCoverageTestMetas(r *rand.Rand, shardIDs []string) []*metadata.Meta {
	var (
		n           = 1 + r.Intn(30)
		pool        = 1 + r.Intn(8)
		resolutions = []int64{ResLevel0, ResLevel1, ResLevel2}
		clusters    = []string{"a", "b", ""}
		metas       = make([]*metadata.Meta, 0, n)
	)
	for i := range n {
		lset := map[string]string{}
		if c := clusters[r.Intn(len(clusters))]; c != "" {
			lset["cluster"] = c
		}
		if shardID := shardIDs[r.Intn(len(shardIDs))]; shardID != "" {
			lset[metadata.CompactorShardIDLabel] = shardID
		}
		var sources []uint64
		for range r.Intn(4) {
			sources = append(sources, uint64(1000+r.Intn(pool)))
		}
		metas = append(metas, coverageTestMeta(uint64(i+1), resolutions[r.Intn(len(resolutions))], lset, sources...))
	}
	return metas
}

func TestCoverage_SameAsUpstreamWithoutShardLabels(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for range 3000 {
		metas := randomCoverageTestMetas(r, []string{""})
		c := NewCoverage()
		for _, m := range metas {
			testutil.Ok(t, c.Add(m))
		}
		for _, m := range metas {
			switch m.Thanos.Downsample.Resolution {
			case ResLevel0:
				testutil.Equals(t, upstreamCovered(metas, m), c.Covers(m, ResLevel1))
			case ResLevel1:
				testutil.Equals(t, upstreamCovered(metas, m), c.Covers(m, ResLevel2))
			}
		}
	}
}

// referenceCovered states the coverage rule directly, per source, as a reference for Coverage.
func referenceCovered(metas []*metadata.Meta, m *metadata.Meta, resolution int64) bool {
	streamOf := func(m *metadata.Meta) (stream string, index, count int) {
		lset := map[string]string{}
		for k, v := range m.Thanos.Labels {
			lset[k] = v
		}
		if v, ok := lset[metadata.CompactorShardIDLabel]; ok {
			var err error
			if index, count, err = metadata.ParseShardID(v); err == nil {
				delete(lset, metadata.CompactorShardIDLabel)
			}
		}
		return labels.FromMap(lset).String(), index, count
	}
	hasSource := func(m *metadata.Meta, id ulid.ULID) bool {
		for _, s := range m.Compaction.Sources {
			if s == id {
				return true
			}
		}
		return false
	}

	stream, _, _ := streamOf(m)
	shardID, sharded := m.Thanos.Labels[metadata.CompactorShardIDLabel]
sources:
	for _, id := range m.Compaction.Sources {
		covering := map[int]map[int]struct{}{}
		for _, d := range metas {
			if d.Thanos.Downsample.Resolution != resolution || !hasSource(d, id) {
				continue
			}
			dShardID, dSharded := d.Thanos.Labels[metadata.CompactorShardIDLabel]
			if !dSharded {
				continue sources
			}
			dStream, index, count := streamOf(d)
			if dStream != stream {
				continue
			}
			if sharded {
				if dShardID == shardID {
					continue sources
				}
				continue
			}
			if count == 0 {
				continue
			}
			if covering[count] == nil {
				covering[count] = map[int]struct{}{}
			}
			covering[count][index] = struct{}{}
			if len(covering[count]) == count {
				continue sources
			}
		}
		return false
	}
	return true
}

func TestCoverage_SameAsReference(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	shardIDs := []string{
		"", "", "", "",
		"1_of_1",
		"1_of_2", "2_of_2", "1_of_2", "2_of_2",
		"1_of_3", "2_of_3", "3_of_3",
		"0_of_2", "01_of_2", "x",
	}
	for range 3000 {
		metas := randomCoverageTestMetas(r, shardIDs)
		c := NewCoverage()
		for _, m := range metas {
			testutil.Ok(t, c.Add(m))
		}
		for _, m := range metas {
			for _, resolution := range []int64{ResLevel1, ResLevel2} {
				testutil.Equals(t, referenceCovered(metas, m, resolution), c.Covers(m, resolution), "block %v at %d", m.Thanos.Labels, resolution)
			}
		}
	}
}

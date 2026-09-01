// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/objtesting"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
)

func newTestFetcherMetrics() *FetcherMetrics {
	return &FetcherMetrics{
		Synced:   extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"}),
		Modified: extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"modified"}),
	}
}

type ulidFilter struct {
	ulidToDelete *ulid.ULID
}

func (f *ulidFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	if _, ok := metas[*f.ulidToDelete]; ok {
		synced.WithLabelValues("filtered").Inc()
		delete(metas, *f.ulidToDelete)
	}
	return nil
}

func ULID(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }

func ULIDs(is ...int) []ulid.ULID {
	ret := []ulid.ULID{}
	for _, i := range is {
		ret = append(ret, ulid.MustNew(uint64(i), nil))
	}

	return ret
}

func TestMetaFetcher_Fetch(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		dir := t.TempDir()

		var ulidToDelete ulid.ULID
		noopLogger := log.NewNopLogger()
		insBkt := objstore.WithNoopInstr(bkt)

		r := prometheus.NewRegistry()

		recursiveLister := NewRecursiveLister(noopLogger, insBkt)
		recursiveBaseFetcher, err := NewBaseFetcher(noopLogger, 20, insBkt, recursiveLister, dir, r)
		testutil.Ok(t, err)

		recursiveFetcher := recursiveBaseFetcher.NewMetaFetcher(r, []MetadataFilter{
			&ulidFilter{ulidToDelete: &ulidToDelete},
		}, nil)

		for _, tcase := range []struct {
			name                  string
			do                    func(cleanCache func())
			filterULID            ulid.ULID
			expectedMetas         []ulid.ULID
			expectedCorruptedMeta []ulid.ULID
			expectedNoMeta        []ulid.ULID
			expectedFiltered      int
			expectedMetaErr       error
			expectedCacheBusts    int
			expectedSyncs         int

			// If this is set then use it.
			fetcher     *MetaFetcher
			baseFetcher *BaseFetcher
		}{
			{
				name: "empty bucket",
				do:   func(_ func()) {},

				expectedMetas:         ULIDs(),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(),
			},
			{
				name: "3 metas in bucket",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 1
					meta.ULID = ULID(1)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))

					meta.ULID = ULID(2)
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))

					meta.ULID = ULID(3)
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(),
			},
			{
				name: "meta 2 and 3 have corrupted data on disk ",
				do: func(cleanCache func()) {
					testutil.Ok(t, os.Remove(filepath.Join(dir, "meta-syncer", ULID(2).String(), MetaFilename)))

					f, err := os.OpenFile(filepath.Join(dir, "meta-syncer", ULID(3).String(), MetaFilename), os.O_WRONLY, os.ModePerm)
					testutil.Ok(t, err)

					_, err = f.WriteString("{ almost")
					testutil.Ok(t, err)
					testutil.Ok(t, f.Close())
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(),
			},
			{
				name: "block without meta",
				do: func(_ func()) {
					testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(4).String(), "some-file"), bytes.NewBuffer([]byte("something"))))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(4),
			},
			{
				name: "corrupted meta.json",
				do: func(_ func()) {
					testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(5).String(), MetaFilename), bytes.NewBuffer([]byte("{ not a json"))))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
			},

			{
				name:       "filter not existing ulid",
				do:         func(_ func()) {},
				filterULID: ULID(10),

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
			},
			{
				name: "filter ulid 1",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 1
					meta.ULID = ULID(1)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},
				filterULID: ULID(1),

				expectedMetas:         ULIDs(2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
				expectedFiltered:      1,
			},
			{
				name: "use recursive lister",
				do: func(cleanCache func()) {
					cleanCache()
				},
				fetcher:     recursiveFetcher,
				baseFetcher: recursiveBaseFetcher,

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
			},
			{
				name: "update timestamp, expect a cache bust",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 1
					meta.MaxTime = 123456
					meta.ULID = ULID(1)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},
				fetcher:     recursiveFetcher,
				baseFetcher: recursiveBaseFetcher,

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
				expectedFiltered:      0,
				expectedCacheBusts:    1,
				expectedSyncs:         2,
			},
			{
				name: "error: not supported meta version",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 20
					meta.ULID = ULID(7)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
				expectedMetaErr:       errors.New("incomplete view: unexpected meta file: 00000000070000000000000000/meta.json version: 20"),
			},
		} {
			if ok := t.Run(tcase.name, func(t *testing.T) {
				r := prometheus.NewRegistry()

				var fetcher *MetaFetcher
				var baseFetcher *BaseFetcher

				if tcase.baseFetcher != nil {
					baseFetcher = tcase.baseFetcher
				} else {
					lister := NewConcurrentLister(noopLogger, insBkt)
					bf, err := NewBaseFetcher(noopLogger, 20, insBkt, lister, dir, r)
					testutil.Ok(t, err)

					baseFetcher = bf
				}

				if tcase.fetcher != nil {
					fetcher = tcase.fetcher
				} else {
					fetcher = baseFetcher.NewMetaFetcher(r, []MetadataFilter{
						&ulidFilter{ulidToDelete: &ulidToDelete},
					}, nil)
				}

				tcase.do(func() {
					baseFetcher.cached.Clear()
					testutil.Ok(t, os.RemoveAll(filepath.Join(dir, "meta-syncer")))
				})

				ulidToDelete = tcase.filterULID
				metas, partial, err := fetcher.Fetch(ctx)
				if tcase.expectedMetaErr != nil {
					testutil.NotOk(t, err)
					testutil.Equals(t, tcase.expectedMetaErr.Error(), err.Error())
				} else {
					testutil.Ok(t, err)
				}

				{
					metasSlice := make([]ulid.ULID, 0, len(metas))
					for id, m := range metas {
						testutil.Assert(t, m != nil, "meta is nil")
						metasSlice = append(metasSlice, id)
					}
					sort.Slice(metasSlice, func(i, j int) bool {
						return metasSlice[i].Compare(metasSlice[j]) < 0
					})
					testutil.Equals(t, tcase.expectedMetas, metasSlice)
				}

				{
					partialSlice := make([]ulid.ULID, 0, len(partial))
					for id := range partial {

						partialSlice = append(partialSlice, id)
					}
					sort.Slice(partialSlice, func(i, j int) bool {
						return partialSlice[i].Compare(partialSlice[j]) >= 0
					})
					expected := append([]ulid.ULID{}, tcase.expectedCorruptedMeta...)
					expected = append(expected, tcase.expectedNoMeta...)
					sort.Slice(expected, func(i, j int) bool {
						return expected[i].Compare(expected[j]) >= 0
					})
					testutil.Equals(t, expected, partialSlice)
				}

				expectedFailures := 0
				if tcase.expectedMetaErr != nil {
					expectedFailures = 1
				}

				testutil.Equals(t, float64(max(1, tcase.expectedSyncs)), promtest.ToFloat64(baseFetcher.syncs))
				testutil.Equals(t, float64(tcase.expectedCacheBusts), promtest.ToFloat64(baseFetcher.cacheBusts))
				testutil.Equals(t, float64(max(1, tcase.expectedSyncs)), promtest.ToFloat64(fetcher.metrics.Syncs))
				testutil.Equals(t, float64(len(tcase.expectedMetas)), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(LoadedMeta)))
				testutil.Equals(t, float64(len(tcase.expectedNoMeta)), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(NoMeta)))
				testutil.Equals(t, float64(tcase.expectedFiltered), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues("filtered")))
				testutil.Equals(t, 0.0, promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(labelExcludedMeta)))
				testutil.Equals(t, 0.0, promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(timeExcludedMeta)))
				testutil.Equals(t, float64(expectedFailures), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(FailedMeta)))
				testutil.Equals(t, 0.0, promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(tooFreshMeta)))
			}); !ok {
				return
			}
		}
	})
}

func TestLabelShardedMetaFilter_Filter_Basic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	relabelContentYaml := `
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    - action: keep
      regex: "keepme"
      source_labels:
      - message
    `
	relabelConfig, err := ParseRelabelConfig([]byte(relabelContentYaml), SelectorSupportedRelabelActions)
	testutil.Ok(t, err)

	f := NewLabelShardedMetaFilter(relabelConfig)

	input := map[ulid.ULID]*metadata.Meta{
		ULID(1): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "B", "message": "keepme"},
			},
		},
		ULID(2): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"something": "A", "message": "keepme"},
			},
		},
		ULID(3): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "A", "message": "keepme"},
			},
		},
		ULID(4): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "A", "something": "B", "message": "keepme"},
			},
		},
		ULID(5): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "B"},
			},
		},
		ULID(6): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "B", "message": "keepme"},
			},
		},
	}
	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): input[ULID(1)],
		ULID(2): input[ULID(2)],
		ULID(6): input[ULID(6)],
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 3.0, promtest.ToFloat64(m.Synced.WithLabelValues(labelExcludedMeta)))
	testutil.Equals(t, expected, input)

}

func TestLabelShardedMetaFilter_Filter_Hashmod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	relabelContentYamlFmt := `
    - action: hashmod
      source_labels: ["%s"]
      target_label: shard
      modulus: 3
    - action: keep
      source_labels: ["shard"]
      regex: %d
`
	for i := range 3 {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			relabelConfig, err := ParseRelabelConfig(fmt.Appendf(nil, relabelContentYamlFmt, BlockIDLabel, i), SelectorSupportedRelabelActions)
			testutil.Ok(t, err)

			f := NewLabelShardedMetaFilter(relabelConfig)

			input := map[ulid.ULID]*metadata.Meta{
				ULID(1): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "B", "message": "keepme"},
					},
				},
				ULID(2): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"something": "A", "message": "keepme"},
					},
				},
				ULID(3): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "A", "message": "keepme"},
					},
				},
				ULID(4): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "A", "something": "B", "message": "keepme"},
					},
				},
				ULID(5): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "B"},
					},
				},
				ULID(6): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "B", "message": "keepme"},
					},
				},
				ULID(7):  {},
				ULID(8):  {},
				ULID(9):  {},
				ULID(10): {},
				ULID(11): {},
				ULID(12): {},
				ULID(13): {},
				ULID(14): {},
				ULID(15): {},
			}
			expected := map[ulid.ULID]*metadata.Meta{}
			switch i {
			case 0:
				expected = map[ulid.ULID]*metadata.Meta{
					ULID(2):  input[ULID(2)],
					ULID(6):  input[ULID(6)],
					ULID(11): input[ULID(11)],
					ULID(13): input[ULID(13)],
				}
			case 1:
				expected = map[ulid.ULID]*metadata.Meta{
					ULID(5):  input[ULID(5)],
					ULID(7):  input[ULID(7)],
					ULID(10): input[ULID(10)],
					ULID(12): input[ULID(12)],
					ULID(14): input[ULID(14)],
					ULID(15): input[ULID(15)],
				}
			case 2:
				expected = map[ulid.ULID]*metadata.Meta{
					ULID(1): input[ULID(1)],
					ULID(3): input[ULID(3)],
					ULID(4): input[ULID(4)],
					ULID(8): input[ULID(8)],
					ULID(9): input[ULID(9)],
				}
			}
			deleted := len(input) - len(expected)

			m := newTestFetcherMetrics()
			testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

			testutil.Equals(t, expected, input)
			testutil.Equals(t, float64(deleted), promtest.ToFloat64(m.Synced.WithLabelValues(labelExcludedMeta)))

		})

	}
}

func TestTimePartitionMetaFilter_Filter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	mint := time.Unix(0, 1*time.Millisecond.Nanoseconds())
	maxt := time.Unix(0, 10*time.Millisecond.Nanoseconds())
	f := NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &mint}, model.TimeOrDurationValue{Time: &maxt})

	input := map[ulid.ULID]*metadata.Meta{
		ULID(1): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 0,
				MaxTime: 1,
			},
		},
		ULID(2): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 1,
				MaxTime: 10,
			},
		},
		ULID(3): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 2,
				MaxTime: 30,
			},
		},
		ULID(4): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 0,
				MaxTime: 30,
			},
		},
		ULID(5): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: -1,
				MaxTime: 0,
			},
		},
		ULID(6): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 20,
				MaxTime: 30,
			},
		},
	}
	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): input[ULID(1)],
		ULID(2): input[ULID(2)],
		ULID(3): input[ULID(3)],
		ULID(4): input[ULID(4)],
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 2.0, promtest.ToFloat64(m.Synced.WithLabelValues(timeExcludedMeta)))
	testutil.Equals(t, expected, input)

}

// resFilterMeta builds a test meta for the resolution filter tests.
func resFilterMeta(resolution int64, lset map[string]string, sources ...ulid.ULID) *metadata.Meta {
	m := &metadata.Meta{}
	m.MinTime, m.MaxTime = 0, 1000
	m.Compaction.Sources = sources
	m.Thanos.Labels = lset
	m.Thanos.Downsample.Resolution = resolution
	return m
}

func TestResolutionMetaFilter_Filter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const res5m, res1h = int64(300000), int64(3600000)
	t1 := map[string]string{"tenant": "1"}
	t2 := map[string]string{"tenant": "2"}

	// Serve only the 5m resolution.
	f := NewResolutionMetaFilter(log.NewNopLogger(), res5m, res5m, nil)

	input := map[ulid.ULID]*metadata.Meta{
		// Kept: in range, and provides coverage for tenant 1 sources 1-3.
		ULID(1): resFilterMeta(res5m, t1, ULIDs(1, 2, 3)...),
		// Dropped: raw, fully covered by the 5m block above.
		ULID(2): resFilterMeta(0, t1, ULIDs(1, 2)...),
		// Kept although raw: source 7 is not covered by any retained block.
		ULID(3): resFilterMeta(0, t1, ULIDs(3, 7)...),
		// Kept although raw: covered sources exist, but only under other labels.
		ULID(4): resFilterMeta(0, t2, ULIDs(1, 2)...),
		// Dropped: coarser than the maximum, no coverage guard on that side.
		ULID(5): resFilterMeta(res1h, t1, ULIDs(1, 2, 3, 7)...),
	}
	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): input[ULID(1)],
		ULID(3): input[ULID(3)],
		ULID(4): input[ULID(4)],
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 2.0, promtest.ToFloat64(m.Synced.WithLabelValues(resolutionExcludedMeta)))
	testutil.Equals(t, expected, input)
}

// TestResolutionMetaFilter_CoveringBlockRemovedByOtherFilter asserts the contract
// behind running the resolution filter last in the store gateway chain: a block
// another filter removed (too fresh, marked for deletion, parquet-migrated) must
// not count as coverage, so the finer block it was built from stays served.
func TestResolutionMetaFilter_CoveringBlockRemovedByOtherFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	raw := ULID(1)
	covering := ULID(2)
	t1 := map[string]string{"tenant": "1"}
	input := map[ulid.ULID]*metadata.Meta{
		raw:      resFilterMeta(0, t1, ULIDs(1)...),
		covering: resFilterMeta(300000, t1, ULIDs(1)...),
	}

	m := newTestFetcherMetrics()
	// E.g. the 5m block is younger than the consistency delay.
	testutil.Ok(t, (&ulidFilter{ulidToDelete: &covering}).Filter(ctx, input, m.Synced, nil))

	f := NewResolutionMetaFilter(log.NewNopLogger(), 300000, 300000, nil)
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 0.0, promtest.ToFloat64(m.Synced.WithLabelValues(resolutionExcludedMeta)))
	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{raw: input[raw]}, input)
}

type sourcesAndResolution struct {
	sources    []ulid.ULID
	resolution int64
}

func TestDeduplicateFilter_Filter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for _, tcase := range []struct {
		name     string
		input    map[ulid.ULID]*sourcesAndResolution
		expected []ulid.ULID
	}{
		{
			name: "3 non compacted blocks in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(2): {
					sources:    []ulid.ULID{ULID(2)},
					resolution: 0,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(3)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(1),
				ULID(2),
				ULID(3),
			},
		},
		{
			name: "compacted block with sources in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(5)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(4),
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "two compacted blocks with same sources",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(5): {
					sources:    []ulid.ULID{ULID(5)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(3),
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "two compacted blocks with overlapping sources",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "3 non compacted blocks and compacted block of level 2 in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(2): {
					sources:    []ulid.ULID{ULID(2)},
					resolution: 0,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(3)},
					resolution: 0,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(2), ULID(1), ULID(3)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(4),
				ULID(6),
			},
		},
		{
			name: "3 compacted blocks of level 2 and one compacted block of level 3 in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(1), ULID(2), ULID(3)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(6), ULID(4), ULID(5)},
					resolution: 0,
				},
				ULID(14): {
					sources:    []ulid.ULID{ULID(14)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(13): {
					sources:    []ulid.ULID{ULID(1), ULID(6), ULID(2), ULID(3), ULID(5), ULID(7), ULID(4), ULID(8), ULID(9)},
					resolution: 0,
				},
				ULID(12): {
					sources:    []ulid.ULID{ULID(7), ULID(9), ULID(8)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(14),
				ULID(13),
			},
		},
		{
			name: "compacted blocks with overlapping sources",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(8): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2), ULID(4)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2), ULID(4)},
					resolution: 0,
				},
				ULID(7): {
					sources:    []ulid.ULID{ULID(3), ULID(1), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(6),
			},
		},
		{
			name: "compacted blocks of level 3 with overlapping sources of equal length",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(1), ULID(2), ULID(6), ULID(7)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(6), ULID(8), ULID(1), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(10),
				ULID(11),
			},
		},
		{
			name: "compacted blocks of level 3 with overlapping sources of different length",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(6), ULID(7), ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(2), ULID(3), ULID(1)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(10),
				ULID(11),
			},
		},
		{
			name: "blocks with same sources and different resolutions",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(2): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 1000,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 10000,
				},
			},
			expected: []ulid.ULID{
				ULID(1),
				ULID(2),
				ULID(3),
			},
		},
		{
			name: "compacted blocks with overlapping sources and different resolutions",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 10000,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(2), ULID(3), ULID(1)},
					resolution: 1000,
				},
			},
			expected: []ulid.ULID{
				ULID(4),
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "compacted blocks of level 3 with overlapping sources of different length and different resolutions",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(7), ULID(5), ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(12): {
					sources:    []ulid.ULID{ULID(6), ULID(7), ULID(1)},
					resolution: 10000,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(13): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 10000,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(2), ULID(3), ULID(1)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(10),
				ULID(11),
				ULID(12),
			},
		},
	} {
		f := NewDeduplicateFilter(1)
		if ok := t.Run(tcase.name, func(t *testing.T) {
			m := newTestFetcherMetrics()
			metas := make(map[ulid.ULID]*metadata.Meta)
			inputLen := len(tcase.input)
			for id, metaInfo := range tcase.input {
				metas[id] = &metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						ULID: id,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: metaInfo.sources,
						},
					},
					Thanos: metadata.Thanos{
						Downsample: metadata.ThanosDownsample{
							Resolution: metaInfo.resolution,
						},
					},
				}
			}
			testutil.Ok(t, f.Filter(ctx, metas, m.Synced, nil))
			compareSliceWithMapKeys(t, metas, tcase.expected)
			testutil.Equals(t, float64(inputLen-len(tcase.expected)), promtest.ToFloat64(m.Synced.WithLabelValues(duplicateMeta)))
		}); !ok {
			return
		}
	}
}

func TestReplicaLabelRemover_Modify(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for _, tcase := range []struct {
		name                string
		input               map[ulid.ULID]*metadata.Meta
		expected            map[ulid.ULID]*metadata.Meta
		modified            float64
		replicaLabelRemover *ReplicaLabelRemover
	}{
		{
			name: "without replica labels",
			input: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			expected: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			modified:            0,
			replicaLabelRemover: NewReplicaLabelRemover(log.NewNopLogger(), []string{"replica", "rule_replica"}),
		},
		{
			name: "with replica labels",
			input: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "cluster1", "message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "cluster1", "rule_replica": "rule1", "message": "something"}}},
				ULID(4): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "cluster1", "rule_replica": "rule1"}}},
			},
			expected: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(4): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "deduped"}}},
			},
			modified:            5.0,
			replicaLabelRemover: NewReplicaLabelRemover(log.NewNopLogger(), []string{"replica", "rule_replica"}),
		},
		{
			name: "no replica label specified in the ReplicaLabelRemover",
			input: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			expected: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			modified:            0,
			replicaLabelRemover: NewReplicaLabelRemover(log.NewNopLogger(), []string{}),
		},
	} {
		m := newTestFetcherMetrics()
		testutil.Ok(t, tcase.replicaLabelRemover.Filter(ctx, tcase.input, nil, m.Modified))

		testutil.Equals(t, tcase.modified, promtest.ToFloat64(m.Modified.WithLabelValues(replicaRemovedMeta)))
		testutil.Equals(t, tcase.expected, tcase.input)
	}
}

func compareSliceWithMapKeys(tb testing.TB, m map[ulid.ULID]*metadata.Meta, s []ulid.ULID) {
	_, file, line, _ := runtime.Caller(1)
	matching := len(m) == len(s)

	for _, val := range s {
		if m[val] == nil {
			matching = false
			break
		}
	}

	if !matching {
		var mapKeys []ulid.ULID
		for id := range m {
			mapKeys = append(mapKeys, id)
		}
		fmt.Printf("\033[31m%s:%d:\n\n\texp keys: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, mapKeys, s)
		tb.FailNow()
	}
}

type ulidBuilder struct {
	entropy *rand.Rand

	created []ulid.ULID
}

func (u *ulidBuilder) ULID(t time.Time) ulid.ULID {
	if u.entropy == nil {
		source := rand.NewSource(1234)
		u.entropy = rand.New(source)
	}

	id := ulid.MustNew(ulid.Timestamp(t), u.entropy)
	u.created = append(u.created, id)
	return id
}

func TestConsistencyDelayMetaFilter_Filter_0(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	u := &ulidBuilder{}
	now := time.Now()

	input := map[ulid.ULID]*metadata.Meta{
		// Fresh blocks.
		u.ULID(now):                       {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},

		// For now non-delay delete sources, should be ignored by consistency delay.
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},

		// 29m.
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},

		// For now non-delay delete sources, should be ignored by consistency delay.
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},

		// 30m.
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},

		// 30m+.
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},
		u.ULID(now):                      {Thanos: metadata.Thanos{UploadTime: time.Now().Add(-20 * time.Hour), Source: metadata.RulerSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},
	}

	t.Run("consistency 0 (turned off)", func(t *testing.T) {
		m := newTestFetcherMetrics()
		expected := map[ulid.ULID]*metadata.Meta{}
		// Copy all.
		for _, id := range u.created {
			expected[id] = input[id]
		}

		reg := prometheus.NewRegistry()
		f := NewConsistencyDelayMetaFilter(nil, 0*time.Second, reg)
		testutil.Equals(t, map[string]float64{"consistency_delay_seconds{}": 0.0}, extprom.CurrentGaugeValuesFor(t, reg, "consistency_delay_seconds"))

		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
		testutil.Equals(t, 0.0, promtest.ToFloat64(m.Synced.WithLabelValues(tooFreshMeta)))
		testutil.Equals(t, expected, input)
	})

	t.Run("consistency 30m.", func(t *testing.T) {
		m := newTestFetcherMetrics()
		expected := map[ulid.ULID]*metadata.Meta{}
		// Only certain sources and those with 30m or more age go through.
		for i, id := range u.created {
			// Younger than 30m.
			if i < 13 {
				if input[id].Thanos.Source != metadata.BucketRepairSource &&
					input[id].Thanos.Source != metadata.CompactorSource &&
					input[id].Thanos.Source != metadata.CompactorRepairSource {
					continue
				}
			}
			expected[id] = input[id]
		}

		reg := prometheus.NewRegistry()
		f := NewConsistencyDelayMetaFilter(nil, 30*time.Minute, reg)
		testutil.Equals(t, map[string]float64{"consistency_delay_seconds{}": (30 * time.Minute).Seconds()}, extprom.CurrentGaugeValuesFor(t, reg, "consistency_delay_seconds"))

		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
		testutil.Equals(t, float64(len(u.created)-len(expected)), promtest.ToFloat64(m.Synced.WithLabelValues(tooFreshMeta)))
		testutil.Equals(t, expected, input)
	})
}

func TestIgnoreDeletionMarkFilter_Filter(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		now := time.Now()
		f := NewIgnoreDeletionMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 48*time.Hour, 32)

		shouldFetch := &metadata.DeletionMark{
			ID:           ULID(1),
			DeletionTime: now.Add(-15 * time.Hour).Unix(),
			Version:      1,
		}

		shouldIgnore := &metadata.DeletionMark{
			ID:           ULID(2),
			DeletionTime: now.Add(-60 * time.Hour).Unix(),
			Version:      1,
		}

		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldFetch))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldFetch.ID.String(), metadata.DeletionMarkFilename), &buf))

		testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldIgnore))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnore.ID.String(), metadata.DeletionMarkFilename), &buf))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(3).String(), metadata.DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))

		input := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(2): {},
			ULID(3): {},
			ULID(4): {},
		}

		expected := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(3): {},
			ULID(4): {},
		}

		m := newTestFetcherMetrics()
		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
		testutil.Equals(t, 1.0, promtest.ToFloat64(m.Synced.WithLabelValues(MarkedForDeletionMeta)))
		testutil.Equals(t, expected, input)
	})
}

func BenchmarkDeduplicateFilter_Filter(b *testing.B) {

	var (
		reg   prometheus.Registerer
		count uint64
		cases []map[ulid.ULID]*metadata.Meta
	)

	dedupFilter := NewDeduplicateFilter(1)
	synced := extprom.NewTxGaugeVec(reg, prometheus.GaugeOpts{}, []string{"state"})

	for blocksNum := 10; blocksNum <= 10000; blocksNum *= 10 {

		var ctx context.Context
		// blocksNum number of blocks with all of them unique ULID and unique 100 sources.
		cases = append(cases, make(map[ulid.ULID]*metadata.Meta, blocksNum))
		for i := 0; i < blocksNum; i++ {

			id := ulid.MustNew(count, nil)
			count++

			cases[0][id] = &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID: id,
				},
			}

			for range 100 {
				cases[0][id].Compaction.Sources = append(cases[0][id].Compaction.Sources, ulid.MustNew(count, nil))
				count++
			}
		}

		// Case for running 3x resolution as they can be run concurrently.
		// blocksNum number of blocks. all of them with unique ULID and unique 100 cases.
		cases = append(cases, make(map[ulid.ULID]*metadata.Meta, 3*blocksNum))

		for i := 0; i < blocksNum; i++ {
			for _, res := range []int64{0, 5 * 60 * 1000, 60 * 60 * 1000} {

				id := ulid.MustNew(count, nil)
				count++
				cases[1][id] = &metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						ULID: id,
					},
					Thanos: metadata.Thanos{
						Downsample: metadata.ThanosDownsample{Resolution: res},
					},
				}
				for range 100 {
					cases[1][id].Compaction.Sources = append(cases[1][id].Compaction.Sources, ulid.MustNew(count, nil))
					count++
				}

			}
		}

		b.Run(fmt.Sprintf("Block-%d", blocksNum), func(b *testing.B) {
			for _, tcase := range cases {
				b.ResetTimer()
				b.Run("", func(b *testing.B) {
					for n := 0; n <= b.N; n++ {
						_ = dedupFilter.Filter(ctx, tcase, synced, nil)
						testutil.Equals(b, 0, len(dedupFilter.DuplicateIDs()))
					}
				})
			}
		})
	}
}

func Test_ParseRelabelConfig(t *testing.T) {
	_, err := ParseRelabelConfig([]byte(`
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    `), SelectorSupportedRelabelActions)
	testutil.Ok(t, err)

	_, err = ParseRelabelConfig([]byte(`
    - action: labelmap
      regex: "A"
    `), SelectorSupportedRelabelActions)
	testutil.NotOk(t, err)
	testutil.Equals(t, "unsupported relabel action: labelmap", err.Error())
}

func TestParquetMigratedMetaFilter_Filter(t *testing.T) {
	logger := log.NewNopLogger()
	filter := NewParquetMigratedMetaFilter(logger)

	// Simulate what might happen when extensions are loaded from JSON
	extensions := struct {
		ParquetMigrated bool `json:"parquet_migrated"`
	}{
		ParquetMigrated: true,
	}

	for _, c := range []struct {
		name  string
		metas map[ulid.ULID]*metadata.Meta
		check func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error)
	}{
		{
			name: "block with other extensions",
			metas: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(2, nil): {
					Thanos: metadata.Thanos{
						Extensions: map[string]any{
							"other_key": "other_value",
						},
					},
				},
			},
			check: func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error) {
				testutil.Ok(t, err)
				testutil.Equals(t, 1, len(metas))
			},
		},
		{
			name: "no extensions",
			metas: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(1, nil): {
					Thanos: metadata.Thanos{
						Extensions: nil,
					},
				},
			},
			check: func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error) {
				testutil.Equals(t, 1, len(metas))
				testutil.Ok(t, err)
			},
		},
		{
			name: "block with parquet_migrated=false",
			metas: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(3, nil): {
					Thanos: metadata.Thanos{
						Extensions: map[string]any{
							metadata.ParquetMigratedExtensionKey: false,
						},
					},
				},
			},
			check: func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error) {
				testutil.Equals(t, 1, len(metas))
				testutil.Ok(t, err)
			},
		},
		{
			name: "block with parquet_migrated=true",
			metas: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(4, nil): {
					Thanos: metadata.Thanos{
						Extensions: map[string]any{
							metadata.ParquetMigratedExtensionKey: true,
						},
					},
				},
			},
			check: func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error) {
				testutil.Equals(t, 0, len(metas))
				testutil.Ok(t, err)
			},
		},
		{
			name: "mixed blocks with parquet_migrated",
			metas: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(5, nil): {
					Thanos: metadata.Thanos{
						Extensions: map[string]any{
							metadata.ParquetMigratedExtensionKey: true,
						},
					},
				},
				ulid.MustNew(6, nil): {
					Thanos: metadata.Thanos{
						Extensions: map[string]any{
							metadata.ParquetMigratedExtensionKey: false,
						},
					},
				},
				ulid.MustNew(7, nil): {
					Thanos: metadata.Thanos{
						Extensions: nil,
					},
				},
			},
			check: func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error) {
				testutil.Equals(t, 2, len(metas))
				testutil.Ok(t, err)
				testutil.Assert(t, metas[ulid.MustNew(6, nil)] != nil, "Expected block with parquet_migrated=false to remain")
				testutil.Assert(t, metas[ulid.MustNew(7, nil)] != nil, "Expected block without extensions to remain")
			},
		},
		{
			name: "block with serialized extensions",
			metas: map[ulid.ULID]*metadata.Meta{
				ulid.MustNew(8, nil): {
					Thanos: metadata.Thanos{
						Extensions: extensions,
					},
				},
			},
			check: func(t *testing.T, metas map[ulid.ULID]*metadata.Meta, err error) {
				testutil.Equals(t, 0, len(metas))
				testutil.Ok(t, err)
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := prometheus.NewRegistry()

			synced := promauto.With(r).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "test_synced",
					Help: "Test synced metric",
				},
				[]string{"state"},
			)
			modified := promauto.With(r).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "test_modified",
					Help: "Test modified metric",
				},
				[]string{"state"},
			)
			ctx := context.Background()

			m, err := json.Marshal(c.metas)
			testutil.Ok(t, err)

			var outmetas map[ulid.ULID]*metadata.Meta
			testutil.Ok(t, json.Unmarshal(m, &outmetas))

			err = filter.Filter(ctx, outmetas, synced, modified)
			c.check(t, outmetas, err)
		})
	}
}

func TestDeletionMarkFilter_HoldsOntoMarks(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	now := time.Now()
	f := NewIgnoreDeletionMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 48*time.Hour, 32)

	shouldFetch := &metadata.DeletionMark{
		ID:           ULID(1),
		DeletionTime: now.Add(-15 * time.Hour).Unix(),
		Version:      1,
	}

	shouldIgnore := &metadata.DeletionMark{
		ID:           ULID(2),
		DeletionTime: now.Add(-60 * time.Hour).Unix(),
		Version:      1,
	}

	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldFetch))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldFetch.ID.String(), metadata.DeletionMarkFilename), &buf))

	buf.Truncate(0)

	md := &metadata.Meta{
		Thanos: metadata.Thanos{
			Version: 1,
		},
	}
	testutil.Ok(t, json.NewEncoder(&buf).Encode(md))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldFetch.ID.String(), "meta.json"), &buf))

	testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldIgnore))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnore.ID.String(), metadata.DeletionMarkFilename), &buf))

	testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(3).String(), metadata.DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))

	input := map[ulid.ULID]*metadata.Meta{
		ULID(1): {},
		ULID(2): {},
		ULID(3): {},
		ULID(4): {},
	}

	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): {},
		ULID(3): {},
		ULID(4): {},
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	testutil.Equals(t, 1.0, promtest.ToFloat64(m.Synced.WithLabelValues(MarkedForDeletionMeta)))
	testutil.Equals(t, expected, input)

	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))

	testutil.Ok(t, bkt.Delete(ctx, path.Join(shouldFetch.ID.String(), metadata.DeletionMarkFilename)))
	input = map[ulid.ULID]*metadata.Meta{
		ULID(1): {},
		ULID(2): {},
		ULID(3): {},
		ULID(4): {},
	}
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))
}

func TestRecursiveLister_MetaJsonOrderIsIrrelevant(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	blockID := ULID(1)

	// Upload files in the order a bucket impl would return them (alphabetical):
	// 1. chunks/000001 ("c" < "m")
	// 2. index ("i" < "m")
	// 3. meta.json
	// 4. no-compact-mark.json ("n" > "m") ← This comes AFTER meta.json
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), "chunks", "000001"), bytes.NewBuffer([]byte("chunks"))))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), "index"), bytes.NewBuffer([]byte("index"))))

	var meta metadata.Meta
	meta.Version = 1
	meta.ULID = blockID
	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), MetaFilename), &buf))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), metadata.NoCompactMarkFilename), bytes.NewBuffer([]byte("{}"))))

	// Create a RecursiveLister
	logger := log.NewNopLogger()
	insBkt := objstore.WithNoopInstr(bkt)
	lister := NewRecursiveLister(logger, insBkt)

	// Get active and partial blocks
	activeBlocksCh := make(chan ActiveBlockFetchData, 10)
	partialBlocks, err := lister.GetActiveAndPartialBlockIDs(ctx, activeBlocksCh)
	testutil.Ok(t, err)
	close(activeBlocksCh)

	// Drain the channel
	var activeBlocks []ulid.ULID
	for block := range activeBlocksCh {
		activeBlocks = append(activeBlocks, block.ULID)
	}

	testutil.Equals(t, 1, len(activeBlocks))
	testutil.Equals(t, blockID, activeBlocks[0])
	isPartial := partialBlocks[blockID]
	if isPartial {
		t.Errorf("Block %s has meta.json but was incorrectly marked as partial", blockID)
	}
}

// TestResolutionMetaFilter_CoverageMustBeAtTheMinimumResolution pins down the
// reachability rule: the query path substitutes finer blocks for missing
// coarser ones but never the other way around, so only a retained block AT the
// minimum resolution can cover a hidden finer block. Coverage that exists only
// at a coarser level is unreachable for a query asking exactly the minimum,
// and the finer block has to stay served.
func TestResolutionMetaFilter_CoverageMustBeAtTheMinimumResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	raw := ULID(1)
	coarse := ULID(2)
	t1 := map[string]string{"tenant": "1"}
	input := map[ulid.ULID]*metadata.Meta{
		// Covered only by a 1h block: a max_source_resolution=5m query could
		// never reach the 1h data, so hiding the raw block would drop the range.
		raw:    resFilterMeta(0, t1, ULIDs(1)...),
		coarse: resFilterMeta(3600000, t1, ULIDs(1)...),
	}

	m := newTestFetcherMetrics()
	f := NewResolutionMetaFilter(log.NewNopLogger(), 300000, 3600000, nil)
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{raw: input[raw], coarse: input[coarse]}, input)
}

// TestResolutionMetaFilter_EmptySourcesIsNeverCovered pins down that a block
// without a recorded genealogy fails open: with no sources to check, the old
// coverage loop was vacuously true and the block was hidden even though
// nothing covers its data - exactly the silent gap the guard exists to prevent.
func TestResolutionMetaFilter_EmptySourcesIsNeverCovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	orphan := ULID(1)
	m5 := ULID(2)
	orphanMeta := &metadata.Meta{}
	orphanMeta.Thanos.Labels = map[string]string{"tenant": "1"}
	orphanMeta.Thanos.Downsample.Resolution = 0 // Raw, and no Compaction.Sources at all.
	coverMeta := &metadata.Meta{}
	coverMeta.Compaction.Sources = ULIDs(1, 2)
	coverMeta.Thanos.Labels = map[string]string{"tenant": "1"}
	coverMeta.Thanos.Downsample.Resolution = 300000

	input := map[ulid.ULID]*metadata.Meta{orphan: orphanMeta, m5: coverMeta}

	m := newTestFetcherMetrics()
	f := NewResolutionMetaFilter(log.NewNopLogger(), 300000, 3600000, nil)
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 2, len(input))
	_, kept := input[orphan]
	testutil.Assert(t, kept, "a block with no source genealogy can never be proven covered and must stay served")
}

// TestResolutionMetaFilter_NoOpRangeShortCircuits pins down that a filter
// admitting every resolution does no work: the default flag values cover
// raw through 1h, and every store gateway pays for this filter on every sync.
func TestResolutionMetaFilter_NoOpRangeShortCircuits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// A block that would be hidden as vacuously covered if the passes ran.
	orphanMeta := &metadata.Meta{}
	orphanMeta.Thanos.Labels = map[string]string{"tenant": "1"}
	input := map[ulid.ULID]*metadata.Meta{ULID(1): orphanMeta}

	m := newTestFetcherMetrics()
	f := NewResolutionMetaFilter(log.NewNopLogger(), 0, 3600000, nil)
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	testutil.Equals(t, 1, len(input))
	testutil.Equals(t, 0.0, promtest.ToFloat64(m.Synced.WithLabelValues(resolutionExcludedMeta)))
}

// TestResolutionMetaFilter_UncoveredGaugeAndBoundedLogging pins down the
// operational surface: the number of served-though-below-minimum blocks is
// exported through a gauge (the alertable form), and it tracks the set as it
// shrinks back to zero.
func TestResolutionMetaFilter_UncoveredGaugeAndBoundedLogging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_uncovered"})
	f := NewResolutionMetaFilter(log.NewNopLogger(), 300000, 3600000, gauge)
	report := f.Reporter()

	t1 := map[string]string{"tenant": "1"}
	m := newTestFetcherMetrics()
	input := map[ulid.ULID]*metadata.Meta{ULID(1): resFilterMeta(0, t1, ULIDs(1)...)}
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	testutil.Ok(t, report.Filter(ctx, input, m.Synced, nil))
	testutil.Equals(t, 1.0, promtest.ToFloat64(gauge))

	// Once coverage exists, the gauge falls back to zero.
	input[ULID(2)] = resFilterMeta(300000, t1, ULIDs(1)...)
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	testutil.Ok(t, report.Filter(ctx, input, m.Synced, nil))
	testutil.Equals(t, 0.0, promtest.ToFloat64(gauge))
}

// TestResolutionMetaFilter_CoverageAcrossTheTimePartition pins the store's
// filter order: the resolution filter runs before the time partition, so a raw
// block straddling --max-time is still proven covered by the 5m block on the far
// side of it, and the reporter runs after the partition, so a raw block that is
// uncovered but not served (out of the window) raises no alarm.
func TestResolutionMetaFilter_CoverageAcrossTheTimePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const res5m, res1h = int64(300000), int64(3600000)
	t1 := map[string]string{"tenant": "1"}
	timed := func(m *metadata.Meta, mint, maxt int64) *metadata.Meta {
		m.MinTime, m.MaxTime = mint, maxt
		return m
	}

	// The store serves [0, 100].
	mint := time.Unix(0, 0)
	maxt := time.Unix(0, 100*time.Millisecond.Nanoseconds())
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_uncovered"})
	resolution := NewResolutionMetaFilter(log.NewNopLogger(), res5m, res1h, gauge)
	chain := []MetadataFilter{
		resolution,
		NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &mint}, model.TimeOrDurationValue{Time: &maxt}),
		resolution.Reporter(),
	}

	input := map[ulid.ULID]*metadata.Meta{
		// Raw block straddling --max-time, built from sources 1-4.
		ULID(1): timed(resFilterMeta(0, t1, ULIDs(1, 2, 3, 4)...), 50, 150),
		// Split 5m cover with shared ancestry: one block inside the window,
		// one beyond it. The two ranges must meet without a gap.
		ULID(2): timed(resFilterMeta(res5m, t1, ULIDs(1, 2, 3, 4)...), 50, 101),
		ULID(3): timed(resFilterMeta(res5m, t1, ULIDs(1, 2, 3, 4)...), 101, 150),
		// Raw block beyond the window nobody downsampled yet.
		ULID(4): timed(resFilterMeta(0, t1, ULIDs(5)...), 200, 250),
		// Raw block inside the window nobody downsampled yet: the one alert.
		ULID(5): timed(resFilterMeta(0, t1, ULIDs(6)...), 0, 50),
	}
	m := newTestFetcherMetrics()
	for _, f := range chain {
		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	}

	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(2): input[ULID(2)], ULID(5): input[ULID(5)]}, input)
	testutil.Equals(t, 1.0, promtest.ToFloat64(gauge))

	// The order the store used before: partition first. The straddling block
	// loses its far-side cover, cannot be proven covered and is served in full,
	// duplicating the range block 2 serves - the regression this test guards.
	input = map[ulid.ULID]*metadata.Meta{
		ULID(1): timed(resFilterMeta(0, t1, ULIDs(1, 2, 3, 4)...), 50, 150),
		ULID(2): timed(resFilterMeta(res5m, t1, ULIDs(1, 2, 3, 4)...), 50, 101),
		ULID(3): timed(resFilterMeta(res5m, t1, ULIDs(1, 2, 3, 4)...), 101, 150),
	}
	for _, f := range []MetadataFilter{chain[1], chain[0], chain[2]} {
		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	}
	testutil.Equals(t, 2, len(input))
	testutil.Equals(t, 1.0, promtest.ToFloat64(gauge))
}

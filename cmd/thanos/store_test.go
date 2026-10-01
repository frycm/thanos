// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func parseStoreFlags(t *testing.T, args ...string) storeConfig {
	t.Helper()
	app := kingpin.New("store", "")
	conf := &storeConfig{}
	conf.registerFlag(app)
	_, err := app.Parse(args)
	testutil.Ok(t, err)
	return *conf
}

// TestStoreFlags_BlockResolutionDefaults pins the defaults to the resolution
// levels the compactor produces: with them no resolution filter is installed,
// so a store gateway started without the flags serves every block as before.
// The levels are milliseconds, and a millisecond count read as a
// time.Duration is nanoseconds, which would turn the 1h maximum into 3.6ms
// and hide every downsampled block by default.
func TestStoreFlags_BlockResolutionDefaults(t *testing.T) {
	conf := parseStoreFlags(t)

	testutil.Equals(t, time.Duration(0), time.Duration(conf.minBlockResolution))
	testutil.Equals(t, time.Hour, time.Duration(conf.maxBlockResolution))
	testutil.Equals(t, downsample.ResLevel2, time.Duration(conf.maxBlockResolution).Milliseconds())
	testutil.Equals(t, false, conf.warnHiddenResolution)
	testutil.Ok(t, validateBlockResolutions(time.Duration(conf.minBlockResolution), time.Duration(conf.maxBlockResolution)))
	testutil.Equals(t, []string{"0s", "5m", "1h"}, listResLevel())
}

func TestStoreFlags_BlockResolutionValidation(t *testing.T) {
	for _, tcase := range []struct {
		name string

		minResolution time.Duration
		maxResolution time.Duration

		expectedErr string
	}{
		{
			name:          "defaults",
			minResolution: 0,
			maxResolution: time.Hour,
		},
		{
			name:          "min equal to max",
			minResolution: 5 * time.Minute,
			maxResolution: 5 * time.Minute,
		},
		{
			// A minimum that is not a downsampling level hides nothing and
			// must be refused; the error must name the levels as durations.
			name:          "min not a downsampling level",
			minResolution: time.Minute,
			maxResolution: time.Hour,
			expectedErr:   "use one of 0s, 5m, 1h",
		},
		{
			name:          "max not a downsampling level",
			minResolution: 0,
			maxResolution: 2 * time.Hour,
			expectedErr:   "is not a downsampling level",
		},
		{
			name:          "sub-millisecond",
			minResolution: 5*time.Minute + time.Microsecond,
			maxResolution: time.Hour,
			expectedErr:   "is not a downsampling level",
		},
		{
			name:          "min above max",
			minResolution: time.Hour,
			maxResolution: 5 * time.Minute,
			expectedErr:   "can't be greater than",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := validateBlockResolutions(tcase.minResolution, tcase.maxResolution)
			if tcase.expectedErr == "" {
				testutil.Ok(t, err)
				return
			}
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tcase.expectedErr), "expected error containing %q, got: %v", tcase.expectedErr, err)
		})
	}
}

func filterTypes(filters []block.MetadataFilter) []string {
	var types []string
	for _, f := range filters {
		types = append(types, strings.TrimPrefix(fmt.Sprintf("%T", f), "*block."))
	}
	return types
}

// TestStoreMetaFilters_Order pins the order of the store gateway's metadata
// filters: with a minimum resolution, the resolution filter runs after every
// filter that can drop a cover for good and before the time partition, which
// the restored fallbacks pass too.
func TestStoreMetaFilters_Order(t *testing.T) {
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	for _, tcase := range []struct {
		name string
		args []string

		expected          []string
		expectedFollowing []string
		expectedFilter    bool
	}{
		{
			name:     "defaults",
			expected: []string{"TimePartitionMetaFilter", "LabelShardedMetaFilter", "ConsistencyDelayMetaFilter", "IgnoreDeletionMarkFilter", "DefaultDeduplicateFilter", "ParquetMigratedMetaFilter"},
		},
		{
			name:              "minimum resolution",
			args:              []string{"--min-block-resolution=5m"},
			expected:          []string{"LabelShardedMetaFilter", "ConsistencyDelayMetaFilter", "IgnoreDeletionMarkFilter", "DefaultDeduplicateFilter", "ParquetMigratedMetaFilter", "ResolutionMetaFilter", "TimePartitionMetaFilter"},
			expectedFollowing: []string{"TimePartitionMetaFilter"},
			expectedFilter:    true,
		},
		{
			name:           "maximum resolution only",
			args:           []string{"--max-block-resolution=5m"},
			expected:       []string{"TimePartitionMetaFilter", "LabelShardedMetaFilter", "ConsistencyDelayMetaFilter", "IgnoreDeletionMarkFilter", "DefaultDeduplicateFilter", "ParquetMigratedMetaFilter", "ResolutionMetaFilter"},
			expectedFilter: true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			conf := parseStoreFlags(t, tcase.args...)
			deletionMarks := block.NewIgnoreDeletionMarkFilter(log.NewNopLogger(), bkt, 0, 1)
			filters, resolution, following := storeMetaFilters(log.NewNopLogger(), prometheus.NewRegistry(), conf, nil, deletionMarks)
			testutil.Equals(t, tcase.expected, filterTypes(filters))
			testutil.Equals(t, tcase.expectedFollowing, filterTypes(following))
			testutil.Equals(t, tcase.expectedFilter, resolution != nil)
			if len(following) > 0 {
				// The very filter the fetcher runs, not a copy with other bounds.
				testutil.Assert(t, filters[len(filters)-1] == following[0], "the fallbacks must pass the fetcher's own time partition")
			}
		})
	}
}

// TestStoreMetaFilters_DroppedCoverHidesNothing runs the store gateway's
// filter chain: a 5m block that the consistency delay or a deletion mark
// drops must not hide the raw block it was made from.
func TestStoreMetaFilters_DroppedCoverHidesNothing(t *testing.T) {
	ctx := t.Context()
	old := time.Now().Add(-2 * time.Hour)
	newID := func(at time.Time, n byte) ulid.ULID {
		return ulid.MustNew(ulid.Timestamp(at), bytes.NewReader(bytes.Repeat([]byte{n}, 16)))
	}
	raw := newID(old, 1)
	meta := func(id ulid.ULID, resolution int64) *metadata.Meta {
		m := &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, MinTime: 0, MaxTime: 3600000, Compaction: tsdb.BlockMetaCompaction{Sources: []ulid.ULID{raw}}}}
		m.Thanos.Labels = map[string]string{"tenant": "1"}
		m.Thanos.Downsample.Resolution = resolution
		return m
	}
	for _, tcase := range []struct {
		name   string
		cover  ulid.ULID
		marked bool
		hidden bool
	}{
		{name: "cover kept", cover: newID(old, 2), hidden: true},
		{name: "cover too fresh", cover: newID(time.Now(), 2)},
		{name: "cover marked for deletion", cover: newID(old, 2), marked: true},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			if tcase.marked {
				body, err := json.Marshal(metadata.DeletionMark{ID: tcase.cover, Version: metadata.DeletionMarkVersion1, DeletionTime: old.Unix()})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(tcase.cover.String(), metadata.DeletionMarkFilename), bytes.NewReader(body)))
			}
			conf := parseStoreFlags(t, "--min-block-resolution=5m", "--consistency-delay=30m")
			filters, _, _ := storeMetaFilters(log.NewNopLogger(), prometheus.NewRegistry(), conf, nil, block.NewIgnoreDeletionMarkFilter(log.NewNopLogger(), bkt, 0, 1))

			metas := map[ulid.ULID]*metadata.Meta{raw: meta(raw, 0), tcase.cover: meta(tcase.cover, downsample.ResLevel1)}
			synced := promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{Name: "synced"}, []string{"state"})
			for _, f := range filters {
				testutil.Ok(t, f.Filter(ctx, metas, synced, synced))
			}
			_, served := metas[raw]
			testutil.Equals(t, !tcase.hidden, served)
		})
	}
}

func TestRelabelUsesBlockID(t *testing.T) {
	for _, tcase := range []struct {
		config   string
		expected bool
	}{
		{config: "", expected: false},
		{config: `
- action: hashmod
  source_labels: ["tenant"]
  target_label: shard
  modulus: 2
- action: keep
  source_labels: ["shard"]
  regex: 0
`, expected: false},
		{config: `
- action: hashmod
  source_labels: ["__block_id"]
  target_label: shard
  modulus: 2
- action: keep
  source_labels: ["shard"]
  regex: 0
`, expected: true},
	} {
		relabelConfig, err := block.ParseRelabelConfig([]byte(tcase.config), block.SelectorSupportedRelabelActions)
		testutil.Ok(t, err)
		testutil.Equals(t, tcase.expected, relabelUsesBlockID(relabelConfig))
	}
}

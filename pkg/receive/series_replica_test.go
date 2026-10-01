// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

const testSeriesReplicaLabel = "prometheus_replica"

func newSeriesReplicaTestMultiTSDB(t *testing.T, dir string, reg prometheus.Registerer, bkt objstore.Bucket, opts ...MultiTSDBOption) *MultiTSDB {
	t.Helper()

	m := NewMultiTSDB(dir, log.NewNopLogger(), reg, &tsdb.Options{
		MinBlockDuration:      (2 * time.Hour).Milliseconds(),
		MaxBlockDuration:      (2 * time.Hour).Milliseconds(),
		RetentionDuration:     (6 * time.Hour).Milliseconds(),
		NoLockfile:            true,
		MaxExemplars:          100,
		EnableExemplarStorage: true,
	},
		labels.FromStrings("receive_replica", "0"),
		"tenant_id",
		bkt,
		false,
		false,
		metadata.NoneFunc,
		opts...,
	)
	return m
}

func testSeries(lbls labels.Labels, ts ...int64) prompb.TimeSeries {
	s := prompb.TimeSeries{Labels: labelpb.ZLabelsFromPromLabels(lbls)}
	for _, t := range ts {
		s.Samples = append(s.Samples, prompb.Sample{Timestamp: t, Value: float64(t)})
	}
	return s
}

// writeFunc writes series of a tenant like a receiver does after routing.
type writeFunc func(t *testing.T, tenant string, series []prompb.TimeSeries) error

func seriesReplicaWriters(m *MultiTSDB) map[string]writeFunc {
	return map[string]writeFunc{
		"protobuf": func(t *testing.T, tenant string, series []prompb.TimeSeries) error {
			return NewWriter(log.NewNopLogger(), m, nil).Write(context.Background(), tenant, series)
		},
		"capnproto": func(t *testing.T, tenant string, series []prompb.TimeSeries) error {
			wr, err := writecapnp.Build(tenant, series)
			require.NoError(t, err)
			req, err := writecapnp.NewRequest(wr)
			require.NoError(t, err)
			defer req.Close()
			return NewCapNProtoWriter(log.NewNopLogger(), m, nil).Write(context.Background(), tenant, req)
		},
	}
}

// localLabelSets returns the label sets of all local StoreAPI clients, sorted.
func localLabelSets(m *MultiTSDB) []string {
	var lsets []string
	for _, c := range m.TSDBLocalClients() {
		for _, lset := range c.LabelSets() {
			lsets = append(lsets, lset.String())
		}
		for _, info := range c.TSDBInfos() {
			if info.Labels.PromLabels().String() != c.LabelSets()[0].String() {
				lsets = append(lsets, "TSDBInfos mismatch: "+info.Labels.PromLabels().String())
			}
		}
	}
	slices.Sort(lsets)
	return lsets
}

// storedSeries returns the series stored in a TSDB, without external labels.
func storedSeries(t *testing.T, m *MultiTSDB, id tsdbID) []string {
	t.Helper()

	m.mtx.RLock()
	tnt := m.tenants[id]
	m.mtx.RUnlock()
	require.NotNil(t, tnt, "no TSDB %s", id)

	q, err := tnt.readyStorage().Get().Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	defer q.Close()

	var series []string
	ss := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	for ss.Next() {
		series = append(series, ss.At().Labels().String())
	}
	require.NoError(t, ss.Err())
	return series
}

func replicaID(tenant, value string) tsdbID {
	return tsdbID{tenant: tenant, replica: labels.Label{Name: testSeriesReplicaLabel, Value: value}}
}

func TestMultiTSDBSeriesReplicaLabel(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"protobuf", "capnproto"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			m := newSeriesReplicaTestMultiTSDB(t, dir, prometheus.NewRegistry(), nil, WithSeriesReplicaLabelName(testSeriesReplicaLabel))
			defer func() { require.NoError(t, m.Close()) }()
			write := seriesReplicaWriters(m)[name]

			r0 := testSeries(labels.FromStrings("__name__", "up", "job", "x", testSeriesReplicaLabel, "r0"), 10, 20)
			r0.Exemplars = []prompb.Exemplar{{Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("trace_id", "abc")), Value: 1, Timestamp: 10}}
			require.NoError(t, write(t, "tenant-a", []prompb.TimeSeries{
				r0,
				testSeries(labels.FromStrings("__name__", "up", "job", "x", testSeriesReplicaLabel, "r1"), 15, 25),
				testSeries(labels.FromStrings("__name__", "up", "job", "y"), 10),
			}))
			require.NoError(t, write(t, "tenant-b", []prompb.TimeSeries{
				testSeries(labels.FromStrings("__name__", "up", "job", "x", testSeriesReplicaLabel, "r0"), 10),
			}))
			// Only series with the label: the tenant's own TSDB is not created.
			require.NoError(t, write(t, "tenant-c", []prompb.TimeSeries{
				testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0"), 10),
			}))
			// A series made of the replica label alone has no labels left.
			err := write(t, "tenant-a", []prompb.TimeSeries{testSeries(labels.FromStrings(testSeriesReplicaLabel, "r0"), 30)})
			require.Error(t, err)
			require.True(t, isConflict(errors.Cause(err)), err)

			require.Equal(t, []string{
				`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-a"}`,
				`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-b"}`,
				`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-c"}`,
				`{prometheus_replica="r1", receive_replica="0", tenant_id="tenant-a"}`,
				`{receive_replica="0", tenant_id="tenant-a"}`,
			}, localLabelSets(m))

			require.Equal(t, []string{`{__name__="up", job="x"}`}, storedSeries(t, m, replicaID("tenant-a", "r0")))
			require.Equal(t, []string{`{__name__="up", job="x"}`}, storedSeries(t, m, replicaID("tenant-a", "r1")))
			require.Equal(t, []string{`{__name__="up", job="y"}`}, storedSeries(t, m, tsdbID{tenant: "tenant-a"}))
			require.Equal(t, []string{`{__name__="up", job="x"}`}, storedSeries(t, m, replicaID("tenant-b", "r0")))
			require.Equal(t, []string{`{__name__="up"}`}, storedSeries(t, m, replicaID("tenant-c", "r0")))

			for _, d := range []string{
				"tenant-a",
				"__series_replicas__/tenant-a/prometheus_replica=7230",
				"__series_replicas__/tenant-a/prometheus_replica=7231",
				"__series_replicas__/tenant-b/prometheus_replica=7230",
				"__series_replicas__/tenant-c/prometheus_replica=7230",
			} {
				require.DirExists(t, filepath.Join(dir, d))
			}
			require.NoDirExists(t, filepath.Join(dir, "tenant-c"))

			// Exemplars of a per-replica TSDB carry its external labels, the replica label included.
			exemplarClients := m.TSDBExemplars()
			require.Len(t, exemplarClients, 5)
			srv := newExemplarsServer(context.Background())
			require.NoError(t, exemplarClients["__series_replicas__/tenant-a/prometheus_replica=7230"].Exemplars(
				[][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "job", "x")}}, 0, 100, srv))
			require.Len(t, srv.Data, 1)
			require.Equal(t,
				`{__name__="up", job="x", prometheus_replica="r0", receive_replica="0", tenant_id="tenant-a"}`,
				srv.Data[0].SeriesLabels.PromLabels().String())

			// Stats are per TSDB, the per-replica TSDBs are listed under their tenant.
			var statsTenants []string
			for _, s := range m.TenantStats(10, labels.MetricName, "tenant-a") {
				statsTenants = append(statsTenants, s.Tenant)
			}
			require.Equal(t, []string{"tenant-a", `tenant-a{prometheus_replica="r0"}`, `tenant-a{prometheus_replica="r1"}`}, statsTenants)
		})
	}
}

func TestMultiTSDBSeriesReplicaRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	m := newSeriesReplicaTestMultiTSDB(t, dir, prometheus.NewRegistry(), nil, WithSeriesReplicaLabelName(testSeriesReplicaLabel))
	w := NewWriter(log.NewNopLogger(), m, nil)
	require.NoError(t, w.Write(context.Background(), "tenant-a", []prompb.TimeSeries{
		testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0"), 10),
		testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "we/ird=..\x00ü"), 10),
		testSeries(labels.FromStrings("__name__", "up", "job", "y"), 10),
	}))
	// A real tenant named like a per-replica TSDB directory stays a tenant.
	require.NoError(t, w.Write(context.Background(), "prometheus_replica=7230", []prompb.TimeSeries{
		testSeries(labels.FromStrings("__name__", "up"), 10),
	}))
	// The directory holding per-replica TSDBs is not a tenant.
	err := w.Write(context.Background(), seriesReplicasDir, []prompb.TimeSeries{testSeries(labels.FromStrings("__name__", "up"), 10)})
	require.Error(t, err)
	require.True(t, isConflict(errors.Cause(err)), err)
	err = w.Write(context.Background(), "./"+seriesReplicasDir+"/tenant-a/prometheus_replica=7230", []prompb.TimeSeries{testSeries(labels.FromStrings("__name__", "up"), 10)})
	require.Error(t, err)
	require.True(t, isConflict(errors.Cause(err)), err)

	expected := []string{
		`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-a"}`,
		`{prometheus_replica="we/ird=..\x00ü", receive_replica="0", tenant_id="tenant-a"}`,
		`{receive_replica="0", tenant_id="prometheus_replica=7230"}`,
		`{receive_replica="0", tenant_id="tenant-a"}`,
	}
	require.Equal(t, expected, localLabelSets(m))
	require.NoError(t, m.Close())

	// Leftover lock files of per-replica TSDBs are removed like those of tenants.
	replicaLock := filepath.Join(dir, seriesReplicasDir, "tenant-a", "prometheus_replica=7230", "lock")
	require.NoError(t, os.WriteFile(replicaLock, nil, 0o600))

	// The per-replica TSDBs come back with their external labels, also when the label is no longer
	// configured, e.g. after a rollback: their data keeps being served and uploaded until pruned.
	for _, opts := range [][]MultiTSDBOption{
		{WithSeriesReplicaLabelName(testSeriesReplicaLabel)},
		{WithSeriesReplicaLabelName("other_replica")},
		nil,
	} {
		reg := prometheus.NewRegistry()
		m := newSeriesReplicaTestMultiTSDB(t, dir, reg, nil, opts...)
		require.NoError(t, m.RemoveLockFilesIfAny())
		require.NoFileExists(t, replicaLock)
		require.NoError(t, m.Open())
		require.Equal(t, expected, localLabelSets(m))
		require.Equal(t, []string{`{__name__="up"}`}, storedSeries(t, m, replicaID("tenant-a", "we/ird=..\x00ü")))
		require.Equal(t, []string{`{__name__="up"}`}, storedSeries(t, m, tsdbID{tenant: "prometheus_replica=7230"}))

		// The TSDB metrics of all TSDBs have the same label names; per-replica ones are told apart.
		mfs, err := reg.Gather()
		require.NoError(t, err)
		var headSeries []string
		for _, mf := range mfs {
			if mf.GetName() != "prometheus_tsdb_head_series" {
				continue
			}
			for _, metric := range mf.GetMetric() {
				var lbls []string
				for _, l := range metric.GetLabel() {
					lbls = append(lbls, l.GetName()+"="+l.GetValue())
				}
				headSeries = append(headSeries, strings.Join(lbls, ","))
			}
		}
		slices.Sort(headSeries)
		require.Equal(t, []string{
			"series_replica=,tenant=prometheus_replica=7230",
			"series_replica=,tenant=tenant-a",
			"series_replica=r0,tenant=tenant-a",
			"series_replica=we/ird=..\x00ü,tenant=tenant-a",
		}, headSeries)
		require.NoError(t, m.Close())
	}
}

func TestMultiTSDBSeriesReplicaUploadAndHashringConfig(t *testing.T) {
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	m := newSeriesReplicaTestMultiTSDB(t, t.TempDir(), prometheus.NewRegistry(), bkt, WithSeriesReplicaLabelName(testSeriesReplicaLabel))
	defer func() { require.NoError(t, m.Close()) }()
	require.NoError(t, m.SetHashringConfig([]HashringConfig{
		{Tenants: []string{"tenant-a"}, ExternalLabels: labels.FromStrings("team", "a", "tenant_id", "ignored")},
	}))

	w := NewWriter(log.NewNopLogger(), m, nil)
	now := time.Now().UnixMilli()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		require.NoError(t, w.Write(context.Background(), tenant, []prompb.TimeSeries{
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0"), now),
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r1"), now),
			testSeries(labels.FromStrings("__name__", "up"), now),
		}))
	}

	require.NoError(t, m.Flush())
	uploaded, err := m.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 6, uploaded)

	require.Equal(t, []string{
		`{prometheus_replica="r0", receive_replica="0", team="a", tenant_id="tenant-a"}`,
		`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-b"}`,
		`{prometheus_replica="r1", receive_replica="0", team="a", tenant_id="tenant-a"}`,
		`{prometheus_replica="r1", receive_replica="0", tenant_id="tenant-b"}`,
		`{receive_replica="0", team="a", tenant_id="tenant-a"}`,
		`{receive_replica="0", tenant_id="tenant-b"}`,
	}, uploadedBlockLabels(t, bkt))

	// A hashring config change updates the per-replica TSDBs of the tenant too.
	require.NoError(t, m.SetHashringConfig([]HashringConfig{
		{Tenants: []string{"tenant-a"}, ExternalLabels: labels.FromStrings("team", "aa", testSeriesReplicaLabel, "ignored")},
	}))
	require.Equal(t, []string{
		`{prometheus_replica="ignored", receive_replica="0", team="aa", tenant_id="tenant-a"}`,
		`{prometheus_replica="r0", receive_replica="0", team="aa", tenant_id="tenant-a"}`,
		`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-b"}`,
		`{prometheus_replica="r1", receive_replica="0", team="aa", tenant_id="tenant-a"}`,
		`{prometheus_replica="r1", receive_replica="0", tenant_id="tenant-b"}`,
		`{receive_replica="0", tenant_id="tenant-b"}`,
	}, localLabelSets(m))
}

func TestMultiTSDBSeriesReplicaPrune(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bkt := objstore.NewInMemBucket()
	m := newSeriesReplicaTestMultiTSDB(t, dir, prometheus.NewRegistry(), bkt, WithSeriesReplicaLabelName(testSeriesReplicaLabel))
	defer func() { require.NoError(t, m.Close()) }()

	w := NewWriter(log.NewNopLogger(), m, nil)
	for step := time.Duration(0); step <= 2*time.Hour; step += time.Minute {
		require.NoError(t, w.Write(context.Background(), "tenant-a", []prompb.TimeSeries{
			// A replica that stopped sending 9h ago.
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "old"), time.Now().Add(-9*time.Hour+step).UnixMilli()),
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0"), time.Now().Add(step).UnixMilli()),
		}))
	}
	require.Len(t, m.TSDBLocalClients(), 2)

	require.NoError(t, m.Prune(context.Background()))
	require.Equal(t, []string{`{prometheus_replica="r0", receive_replica="0", tenant_id="tenant-a"}`}, localLabelSets(m))
	require.NoDirExists(t, filepath.Join(dir, "__series_replicas__/tenant-a/prometheus_replica=6f6c64"))
	require.DirExists(t, filepath.Join(dir, "__series_replicas__/tenant-a/prometheus_replica=7230"))

	// The pruned TSDB was uploaded before it was deleted.
	blockLabels := uploadedBlockLabels(t, bkt)
	require.NotEmpty(t, blockLabels)
	for _, lset := range blockLabels {
		require.Equal(t, `{prometheus_replica="old", receive_replica="0", tenant_id="tenant-a"}`, lset)
	}
}

// uploadedBlockLabels returns the external labels of all blocks in the bucket, sorted.
func uploadedBlockLabels(t *testing.T, bkt objstore.Bucket) []string {
	t.Helper()

	var blockLabels []string
	require.NoError(t, bkt.Iter(context.Background(), "", func(name string) error {
		r, err := bkt.Get(context.Background(), name+metadata.MetaFilename)
		if err != nil {
			return err
		}
		defer r.Close()
		var meta metadata.Meta
		if err := json.NewDecoder(r).Decode(&meta); err != nil {
			return err
		}
		blockLabels = append(blockLabels, labels.FromMap(meta.Thanos.Labels).String())
		return nil
	}))
	slices.Sort(blockLabels)
	return blockLabels
}

func TestTSDBIDValidate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		id    tsdbID
		valid bool
	}{
		{id: tsdbID{tenant: "tenant"}, valid: true},
		{id: tsdbID{tenant: "prometheus_replica=7230"}, valid: true},
		{id: tsdbID{tenant: seriesReplicasDir}},
		{id: tsdbID{tenant: seriesReplicasDir + "/tenant/prometheus_replica=7230"}},
		{id: tsdbID{tenant: "a/../" + seriesReplicasDir}},
		{id: replicaID("tenant", "r0"), valid: true},
		{id: replicaID(seriesReplicasDir, "r0"), valid: true},
		{id: replicaID("tenant", strings.Repeat("v", 118)), valid: true},
		{id: replicaID("tenant", strings.Repeat("v", 119))},
		{id: replicaID("tenant", "")},
		{id: replicaID("", "r0")},
		{id: replicaID(".", "r0")},
		{id: replicaID("..", "r0")},
		{id: replicaID("a/b", "r0")},
	} {
		t.Run(tc.id.dir(), func(t *testing.T) {
			err := tc.id.validate()
			if tc.valid {
				require.NoError(t, err)
				if tc.id.isReplica() {
					replica, err := parseSeriesReplicaDirName(filepath.Base(tc.id.dir()))
					require.NoError(t, err)
					require.Equal(t, tc.id.replica, replica)
				}
				return
			}
			require.Error(t, err)
			require.True(t, isConflict(errors.Cause(err)))
		})
	}

	for _, name := range []string{"prometheus_replica", "prometheus_replica=", "prometheus_replica=7", "prometheus_replica=zz", "1abc=7230", "wal"} {
		_, err := parseSeriesReplicaDirName(name)
		require.Error(t, err, name)
	}
}

func TestMultiTSDBSeriesReplicaIgnoresUnexpectedDirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, seriesReplicasDir, "tenant-a", "wal"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, seriesReplicasDir, "file"), nil, 0o600))

	m := newSeriesReplicaTestMultiTSDB(t, dir, prometheus.NewRegistry(), nil)
	defer func() { require.NoError(t, m.Close()) }()
	require.NoError(t, m.Open())
	require.Empty(t, m.TSDBLocalClients())
}

func TestHandlerSeriesReplicaRouting(t *testing.T) {
	t.Parallel()

	var endpoints []Endpoint
	for _, addr := range []string{"a:1", "b:1", "c:1", "d:1", "e:1"} {
		endpoints = append(endpoints, Endpoint{Address: addr})
	}

	for _, tc := range []struct {
		name        string
		splitTenant string
	}{
		{name: "tenant from header"},
		{name: "tenant from split tenant label", splitTenant: "tenant_label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(nil, &Options{
				Endpoint:               "local:1",
				ReplicationFactor:      1,
				SplitTenantLabelName:   tc.splitTenant,
				SeriesReplicaLabelName: testSeriesReplicaLabel,
			})
			hr, err := newKetamaHashring(endpoints, 10, 1)
			require.NoError(t, err)
			h.Hashring(hr)

			var series []prompb.TimeSeries
			for i := range 50 {
				for _, replica := range []string{"r0", "r1", "a-much-longer-replica-name"} {
					lbls := labels.FromStrings("__name__", "up", "i", strings.Repeat("x", i), testSeriesReplicaLabel, replica)
					if tc.splitTenant != "" {
						lbls = labels.NewBuilder(lbls).Set(tc.splitTenant, "split").Labels()
					}
					series = append(series, testSeries(lbls))
				}
			}

			_, remote, err := h.distributeTimeseriesToReplicas("header", []uint64{0}, series)
			require.NoError(t, err)

			expectedTenant := "header"
			if tc.splitTenant != "" {
				expectedTenant = "split"
			}
			endpointOf := map[string]string{}
			var routed int
			for er, tenants := range remote {
				require.Len(t, tenants, 1)
				tracked, ok := tenants[expectedTenant]
				require.True(t, ok)
				for _, ts := range tracked.timeSeries {
					routed++
					lbls := labelpb.ZLabelsToPromLabels(ts.Labels)
					// The receiver that writes the series removes the label, so it is still sent.
					require.NotEmpty(t, lbls.Get(testSeriesReplicaLabel))
					require.Empty(t, lbls.Get("tenant_label"))
					key := lbls.Get("i")
					if prev, ok := endpointOf[key]; ok {
						require.Equal(t, prev, er.endpoint.Address, "replicas of series %s routed to different endpoints", key)
					}
					endpointOf[key] = er.endpoint.Address
				}
			}
			require.Equal(t, len(series), routed)
		})
	}
}

func TestHandlerSeriesReplicaLocalWrite(t *testing.T) {
	t.Parallel()

	m := newSeriesReplicaTestMultiTSDB(t, t.TempDir(), prometheus.NewRegistry(), nil, WithSeriesReplicaLabelName(testSeriesReplicaLabel))
	defer func() { require.NoError(t, m.Close()) }()

	h := NewHandler(nil, &Options{
		Endpoint:               "localhost",
		ReceiverMode:           RouterIngestor,
		ReplicationFactor:      1,
		ForwardTimeout:         time.Second,
		SplitTenantLabelName:   "tenant_label",
		SeriesReplicaLabelName: testSeriesReplicaLabel,
		Writer:                 NewWriter(log.NewNopLogger(), m, nil),
	})
	hr, err := newSimpleHashring([]Endpoint{{Address: h.options.Endpoint}})
	require.NoError(t, err)
	h.Hashring(hr)

	_, err = h.RemoteWrite(context.Background(), &storepb.WriteRequest{
		Tenant: "header",
		Timeseries: []prompb.TimeSeries{
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0"), 10),
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0", "tenant_label", "split"), 10),
			testSeries(labels.FromStrings("__name__", "up", "tenant_label", "split"), 10),
		},
	})
	require.NoError(t, err)

	require.Equal(t, []string{
		`{prometheus_replica="r0", receive_replica="0", tenant_id="header"}`,
		`{prometheus_replica="r0", receive_replica="0", tenant_id="split"}`,
		`{receive_replica="0", tenant_id="split"}`,
	}, localLabelSets(m))
	require.Equal(t, []string{`{__name__="up"}`}, storedSeries(t, m, replicaID("split", "r0")))
	require.Equal(t, []string{`{__name__="up"}`}, storedSeries(t, m, replicaID("header", "r0")))
}

// TestSeriesReplicaQueryDeduplication checks that a querier deduplicates the per-replica TSDBs of a
// receiver by the series replica label, like it deduplicates Prometheus replicas by external labels,
// while it never merges different tenants.
func TestSeriesReplicaQueryDeduplication(t *testing.T) {
	t.Parallel()

	m := newSeriesReplicaTestMultiTSDB(t, t.TempDir(), prometheus.NewRegistry(), nil, WithSeriesReplicaLabelName(testSeriesReplicaLabel))
	defer func() { require.NoError(t, m.Close()) }()

	w := NewWriter(log.NewNopLogger(), m, nil)
	var r0, r1 []int64
	for ts := int64(0); ts < 600_000; ts += 15_000 {
		r0 = append(r0, ts)
		// The second replica started 5 minutes later, its samples are shifted by 1s.
		if ts >= 300_000 {
			r1 = append(r1, ts+1000)
		}
	}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		require.NoError(t, w.Write(context.Background(), tenant, []prompb.TimeSeries{
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r0"), r0...),
			testSeries(labels.FromStrings("__name__", "up", testSeriesReplicaLabel, "r1"), r1...),
		}))
	}

	proxy := store.NewProxyStore(nil, nil, m.TSDBLocalClients, component.Query, labels.EmptyLabels(), time.Minute, store.EagerRetrieval)
	queryableCreator := query.NewQueryableCreator(nil, nil, proxy, 2, time.Minute, dedup.AlgorithmPenalty, 1)

	selectUp := func(deduplicate bool) map[string]int {
		q, err := queryableCreator(deduplicate, []string{testSeriesReplicaLabel}, nil, 0, false, false, nil, query.NoopSeriesStatsReporter).Querier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		defer q.Close()

		samples := map[string]int{}
		ss := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))
		for ss.Next() {
			it := ss.At().Iterator(nil)
			for it.Next() != 0 {
				samples[ss.At().Labels().String()]++
			}
			require.NoError(t, it.Err())
		}
		require.NoError(t, ss.Err())
		return samples
	}

	require.Equal(t, map[string]int{
		`{__name__="up", prometheus_replica="r0", receive_replica="0", tenant_id="tenant-a"}`: len(r0),
		`{__name__="up", prometheus_replica="r1", receive_replica="0", tenant_id="tenant-a"}`: len(r1),
		`{__name__="up", prometheus_replica="r0", receive_replica="0", tenant_id="tenant-b"}`: len(r0),
		`{__name__="up", prometheus_replica="r1", receive_replica="0", tenant_id="tenant-b"}`: len(r1),
	}, selectUp(false))
	require.Equal(t, map[string]int{
		`{__name__="up", receive_replica="0", tenant_id="tenant-a"}`: len(r0),
		`{__name__="up", receive_replica="0", tenant_id="tenant-b"}`: len(r0),
	}, selectUp(true))
}

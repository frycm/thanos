// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/receive/expandedpostingscache"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type TSDBStats interface {
	// TenantStats returns TSDB head stats for the given tenants.
	// If no tenantIDs are provided, stats for all tenants are returned.
	TenantStats(limit int, statsByLabelName string, tenantIDs ...string) []api.TenantStats
}

type MultiTSDB struct {
	dataDir         string
	logger          log.Logger
	reg             prometheus.Registerer
	tsdbOpts        *tsdb.Options
	tenantLabelName string
	labels          labels.Labels
	bucket          objstore.Bucket

	// seriesReplicaLabelName is the series label whose value selects a per-replica TSDB of the
	// tenant; empty when disabled. See WithSeriesReplicaLabelName.
	seriesReplicaLabelName string
	// seriesReplicaMetricLabel tells whether the metrics of every TSDB carry the
	// seriesReplicaMetricLabel label next to the tenant label.
	seriesReplicaMetricLabel bool

	mtx *sync.RWMutex
	// tenants holds all TSDBs: the TSDB of each tenant and the per-replica TSDBs of tenants.
	tenants               map[tsdbID]*tenant
	allowOutOfOrderUpload bool
	skipCorruptedBlocks   bool
	hashFunc              metadata.HashFunc
	uploadConcurrency     int

	hashringConfigsMtx sync.RWMutex
	hashringConfigs    []HashringConfig

	matcherCache storecache.MatchersCache

	tsdbClients     []store.Client
	exemplarClients map[string]*exemplars.TSDB

	metricNameFilterEnabled bool

	headExpandedPostingsCacheSize  uint64
	blockExpandedPostingsCacheSize uint64

	initSingleFlight singleflight.Group
}

// MultiTSDBOption is a functional option for MultiTSDB.
type MultiTSDBOption func(mt *MultiTSDB)

// WithMetricNameFilterEnabled enables metric name filtering on TSDB clients.
func WithMetricNameFilterEnabled() MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.metricNameFilterEnabled = true
	}
}

func WithHeadExpandedPostingsCacheSize(size uint64) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.headExpandedPostingsCacheSize = size
	}
}

func WithBlockExpandedPostingsCacheSize(size uint64) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.blockExpandedPostingsCacheSize = size
	}
}

func WithMatchersCache(cache storecache.MatchersCache) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.matcherCache = cache
	}
}

func WithUploadConcurrency(concurrency int) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.uploadConcurrency = concurrency
	}
}

// WithSeriesReplicaLabelName makes the MultiTSDB keep, for each tenant and each value of the given
// series label, a separate TSDB whose external labels are the tenant's ones plus that label and
// value. Writers remove the label from such series and append them through TenantReplicaAppendable.
// The name must be a valid legacy Prometheus label name.
func WithSeriesReplicaLabelName(name string) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.seriesReplicaLabelName = name
	}
}

// NewMultiTSDB creates new MultiTSDB.
// NOTE: Passed labels must be sorted lexicographically (alphabetically).
func NewMultiTSDB(
	dataDir string,
	l log.Logger,
	reg prometheus.Registerer,
	tsdbOpts *tsdb.Options,
	labels labels.Labels,
	tenantLabelName string,
	bucket objstore.Bucket,
	allowOutOfOrderUpload bool,
	skipCorruptedBlocks bool,
	hashFunc metadata.HashFunc,
	options ...MultiTSDBOption,
) *MultiTSDB {
	if l == nil {
		l = log.NewNopLogger()
	}

	mt := &MultiTSDB{
		dataDir:               dataDir,
		logger:                log.With(l, "component", "multi-tsdb"),
		reg:                   reg,
		tsdbOpts:              tsdbOpts,
		mtx:                   &sync.RWMutex{},
		tenants:               map[tsdbID]*tenant{},
		labels:                labels,
		tsdbClients:           make([]store.Client, 0),
		exemplarClients:       map[string]*exemplars.TSDB{},
		tenantLabelName:       tenantLabelName,
		bucket:                bucket,
		allowOutOfOrderUpload: allowOutOfOrderUpload,
		skipCorruptedBlocks:   skipCorruptedBlocks,
		hashFunc:              hashFunc,
		uploadConcurrency:     0,
		matcherCache:          storecache.NoopMatchersCache,
	}

	for _, option := range options {
		option(mt)
	}

	// All TSDB metrics registered in one registry need the same label names. Per-replica TSDBs are
	// told apart by an extra label, so add it to every TSDB whenever there can be per-replica TSDBs:
	// when they are enabled, or when some are left on disk (they are loaded even when disabled).
	mt.seriesReplicaMetricLabel = mt.seriesReplicaLabelName != ""
	if fi, err := os.Stat(filepath.Join(dataDir, seriesReplicasDir)); err == nil && fi.IsDir() {
		mt.seriesReplicaMetricLabel = true
	}

	return mt
}

const (
	// seriesReplicasDir is the directory, inside the data directory, holding the per-replica TSDBs
	// at <seriesReplicasDir>/<tenant>/<label name>=<hex-encoded label value>. The name is reserved:
	// it is never loaded as a tenant and writes for a tenant of that name are rejected.
	seriesReplicasDir = "__series_replicas__"
	// seriesReplicaMetricLabel is the metric label that holds the series replica label value of a
	// per-replica TSDB next to the tenant label (empty for the tenant's own TSDB).
	seriesReplicaMetricLabel = "series_replica"
	// maxDirNameLength is the maximum length of a file name on common file systems.
	maxDirNameLength = 255
)

// errInvalidTSDBName is returned for writes whose tenant or series replica label value cannot name a
// TSDB directory. Such writes are rejected as conflicts, retrying them cannot succeed.
var errInvalidTSDBName = errors.New("cannot store series in a TSDB of this name")

// tsdbID identifies a TSDB of a MultiTSDB: either the TSDB of a tenant or, when a series replica
// label is used, a per-replica TSDB of a tenant.
type tsdbID struct {
	tenant string
	// replica is the series replica label (name and value) of a per-replica TSDB: it was removed from
	// the TSDB's series and is one of its external labels. It is empty for the tenant's own TSDB.
	replica labels.Label
}

func (id tsdbID) isReplica() bool {
	return id.replica.Name != ""
}

// dir returns the TSDB directory relative to the data directory. It is unique per TSDB.
func (id tsdbID) dir() string {
	if !id.isReplica() {
		return id.tenant
	}
	return seriesReplicasDir + "/" + id.tenant + "/" + seriesReplicaDirName(id.replica)
}

// String returns a human readable name of the TSDB, e.g. `tenant` or `tenant{replica="r0"}`.
func (id tsdbID) String() string {
	if !id.isReplica() {
		return id.tenant
	}
	return id.tenant + labels.New(id.replica).String()
}

// validate checks that id can be stored in its own directory without clashing with another TSDB.
func (id tsdbID) validate() error {
	if !id.isReplica() {
		if d := path.Clean(id.tenant); d == seriesReplicasDir || strings.HasPrefix(d, seriesReplicasDir+"/") {
			return errors.Wrapf(errInvalidTSDBName, "tenant name %q is reserved", id.tenant)
		}
		return nil
	}
	switch {
	case id.tenant == "", id.tenant == ".", id.tenant == "..", strings.ContainsAny(id.tenant, "/\x00"), len(id.tenant) > maxDirNameLength:
		return errors.Wrapf(errInvalidTSDBName, "tenant %q is not a valid directory name for its per-replica TSDBs", id.tenant)
	case id.replica.Value == "":
		return errors.Wrapf(errInvalidTSDBName, "empty value of series replica label %q", id.replica.Name)
	case len(seriesReplicaDirName(id.replica)) > maxDirNameLength:
		return errors.Wrapf(errInvalidTSDBName, "value of series replica label %q is too long (%d bytes, at most %d)",
			id.replica.Name, len(id.replica.Value), (maxDirNameLength-len(id.replica.Name)-1)/2)
	}
	return nil
}

// seriesReplicaDirName returns the directory name of a per-replica TSDB. The value is hex-encoded so
// that any label value gives a valid name, also on case-insensitive file systems.
func seriesReplicaDirName(replica labels.Label) string {
	return replica.Name + "=" + hex.EncodeToString([]byte(replica.Value))
}

func parseSeriesReplicaDirName(name string) (labels.Label, error) {
	n, v, ok := strings.Cut(name, "=")
	if !ok || !model.LegacyValidation.IsValidLabelName(n) {
		return labels.Label{}, errors.Errorf("%q is not of the form <label name>=<hex-encoded label value>", name)
	}
	value, err := hex.DecodeString(v)
	if err != nil {
		return labels.Label{}, errors.Wrapf(err, "decode label value of %q", name)
	}
	if len(value) == 0 {
		return labels.Label{}, errors.Errorf("empty label value in %q", name)
	}
	return labels.Label{Name: n, Value: string(value)}, nil
}

// testGetTenant returns the tenant with the given tenantID for testing purposes.
func (t *MultiTSDB) testGetTenant(tenantID string) *tenant {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.tenants[tsdbID{tenant: tenantID}]
}

func (t *MultiTSDB) updateTSDBClients() {
	t.tsdbClients = t.tsdbClients[:0]
	for _, tenant := range t.tenants {
		client := tenant.client()
		if client != nil {
			t.tsdbClients = append(t.tsdbClients, client)
		}
	}
}

func (t *MultiTSDB) addTenantUnlocked(id tsdbID, newTenant *tenant) {
	t.tenants[id] = newTenant
	t.updateTSDBClients()
	if newTenant.exemplars() != nil {
		t.exemplarClients[id.dir()] = newTenant.exemplars()
	}
}

func (t *MultiTSDB) addTenantLocked(id tsdbID, newTenant *tenant) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.addTenantUnlocked(id, newTenant)
}

func (t *MultiTSDB) removeTenantUnlocked(id tsdbID) {
	delete(t.tenants, id)
	delete(t.exemplarClients, id.dir())
	t.updateTSDBClients()
}

func (t *MultiTSDB) removeTenantLocked(id tsdbID) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.removeTenantUnlocked(id)
}

// tsdbLogger returns a logger with the tenant and, for a per-replica TSDB, the series replica label.
func (t *MultiTSDB) tsdbLogger(id tsdbID) log.Logger {
	if !id.isReplica() {
		return log.With(t.logger, "tenant", id.tenant)
	}
	return log.With(t.logger, "tenant", id.tenant, "series_replica", labels.New(id.replica).String())
}

type localClient struct {
	store *store.TSDBStore

	client storepb.StoreClient
}

func newLocalClient(store *store.TSDBStore, readOnly atomic.Bool) *localClient {
	return &localClient{
		store:  store,
		client: storepb.ServerAsClient(store, readOnly),
	}
}

func (l *localClient) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return l.client.Series(ctx, in, opts...)
}

func (l *localClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return l.store.LabelNames(ctx, in)
}

func (l *localClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return l.store.LabelValues(ctx, in)
}

func (l *localClient) Matches(matchers []*labels.Matcher) bool {
	return l.store.Matches(matchers)
}

func (l *localClient) LabelSets() []labels.Labels {
	return labelpb.ZLabelSetsToPromLabelSets(l.store.LabelSet()...)
}

func (l *localClient) TimeRange() (mint int64, maxt int64) {
	return l.store.TimeRange()
}

func (l *localClient) TSDBInfos() []infopb.TSDBInfo {
	labelsets := l.store.LabelSet()
	if len(labelsets) == 0 {
		return []infopb.TSDBInfo{}
	}

	mint, maxt := l.store.TimeRange()
	return []infopb.TSDBInfo{
		{
			Labels:  labelsets[0],
			MinTime: mint,
			MaxTime: maxt,
		},
	}
}

func (l *localClient) String() string {
	mint, maxt := l.store.TimeRange()
	return fmt.Sprintf(
		"MinTime: %d MaxTime: %d",
		mint, maxt,
	)
}

func (l *localClient) Addr() (string, bool) {
	return "", true
}

func (l *localClient) SupportsSharding() bool {
	return true
}

func (l *localClient) SupportsWithoutReplicaLabels() bool {
	return true
}

func (t *tenant) setReadOnly(ro bool) {
	t.readOnly.Store(ro)
}

type tenant struct {
	readyS        *ReadyStorage
	storeTSDB     *store.TSDBStore
	exemplarsTSDB *exemplars.TSDB
	ship          *shipper.Shipper
	reg           *UnRegisterer

	readOnly atomic.Bool

	mtx  *sync.RWMutex
	tsdb *tsdb.DB

	// For tests.
	blocksToDeleteFn func(db *tsdb.DB) tsdb.BlocksToDeleteFunc
}

func (m *MultiTSDB) initTSDBIfNeeded(id tsdbID, t *tenant) error {
	_, err, _ := m.initSingleFlight.Do(id.dir(), func() (interface{}, error) {
		if t.readyS.Get() != nil {
			return nil, nil
		}
		return nil, m.startTSDB(m.tsdbLogger(id), id, t)
	})

	return err
}

func (t *tenant) blocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.tsdb == nil {
		return nil
	}

	deletable := t.blocksToDeleteFn(t.tsdb)(blocks)
	if t.ship == nil {
		return deletable
	}

	uploaded := t.ship.UploadedBlocks()
	for deletableID := range deletable {
		if _, ok := uploaded[deletableID]; !ok {
			delete(deletable, deletableID)
		}
	}

	return deletable
}

func newTenant() *tenant {
	return &tenant{
		readyS: &ReadyStorage{},
		mtx:    &sync.RWMutex{},
	}
}

func (t *tenant) readyStorage() *ReadyStorage {
	return t.readyS
}

func (t *tenant) client() store.Client {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	tsdbStore := t.storeTSDB
	if tsdbStore == nil {
		return nil
	}

	return newLocalClient(tsdbStore, t.readOnly)
}

func (t *tenant) exemplars() *exemplars.TSDB {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.exemplarsTSDB
}

func (t *tenant) shipper() *shipper.Shipper {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.ship
}

func (t *tenant) set(storeTSDB *store.TSDBStore, tenantTSDB *tsdb.DB, ship *shipper.Shipper, exemplarsTSDB *exemplars.TSDB, reg *UnRegisterer) {
	t.readyS.Set(tenantTSDB)
	t.mtx.Lock()
	t.setComponents(storeTSDB, ship, exemplarsTSDB, tenantTSDB, reg)
	t.mtx.Unlock()
}

func (t *tenant) setComponents(storeTSDB *store.TSDBStore, ship *shipper.Shipper, exemplarsTSDB *exemplars.TSDB, tenantTSDB *tsdb.DB, reg *UnRegisterer) {
	if storeTSDB == nil && t.storeTSDB != nil {
		t.storeTSDB.Close()
	}
	if reg == nil && t.reg != nil {
		t.reg.UnregisterAll()
	}
	t.storeTSDB = storeTSDB
	t.reg = reg
	t.ship = ship
	t.exemplarsTSDB = exemplarsTSDB
	t.tsdb = tenantTSDB
}

// setExtLabels changes the external labels of the TSDB's StoreAPI, exemplars and future uploads.
func (t *tenant) setExtLabels(lset labels.Labels) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.ship != nil {
		t.ship.SetLabels(lset)
	}
	if t.storeTSDB != nil {
		t.storeTSDB.SetExtLset(lset)
	}
	if t.exemplarsTSDB != nil {
		t.exemplarsTSDB.SetExtLabels(lset)
	}
}

func (t *MultiTSDB) Open() error {
	if err := os.MkdirAll(t.dataDir, 0750); err != nil {
		return err
	}

	ids, err := t.discoverTSDBs()
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, id := range ids {
		g.Go(func() error {
			_, err := t.getOrLoadTenant(id)
			return err
		})
	}

	return g.Wait()
}

// discoverTSDBs returns the TSDBs found in the data directory: every directory is the TSDB of the
// tenant it is named after, except seriesReplicasDir, which holds per-replica TSDBs. Per-replica
// TSDBs are found even if the series replica label is no longer configured, so their data keeps
// being uploaded and served until they are pruned.
func (t *MultiTSDB) discoverTSDBs() ([]tsdbID, error) {
	entries, err := os.ReadDir(t.dataDir)
	if err != nil {
		return nil, err
	}

	var ids []tsdbID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if e.Name() != seriesReplicasDir {
			ids = append(ids, tsdbID{tenant: e.Name()})
			continue
		}

		root := filepath.Join(t.dataDir, seriesReplicasDir)
		tenantDirs, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		for _, td := range tenantDirs {
			if !td.IsDir() {
				continue
			}
			replicaDirs, err := os.ReadDir(filepath.Join(root, td.Name()))
			if err != nil {
				return nil, err
			}
			for _, rd := range replicaDirs {
				if !rd.IsDir() {
					continue
				}
				replica, err := parseSeriesReplicaDirName(rd.Name())
				if err != nil {
					level.Warn(t.logger).Log("msg", "ignoring unexpected directory among per-replica TSDBs", "dir", filepath.Join(root, td.Name(), rd.Name()), "err", err)
					continue
				}
				ids = append(ids, tsdbID{tenant: td.Name(), replica: replica})
			}
		}
	}
	return ids, nil
}

func (t *MultiTSDB) Flush() error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	errmtx := &sync.Mutex{}
	merr := errutil.MultiError{}
	wg := &sync.WaitGroup{}
	for id, tenant := range t.tenants {
		db := tenant.readyStorage().Get()
		if db == nil {
			level.Error(t.logger).Log("msg", "flushing TSDB failed; not ready", "tenant", id)
			continue
		}
		level.Info(t.logger).Log("msg", "flushing TSDB", "tenant", id)
		wg.Go(func() {
			if err := t.flushHead(db); err != nil {
				errmtx.Lock()
				merr.Add(err)
				errmtx.Unlock()
			}
		})
	}

	wg.Wait()
	return merr.Err()
}

func (t *MultiTSDB) flushHead(db *tsdb.DB) error {
	head := db.Head()
	if head.MinTime() == head.MaxTime() {
		return db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime()))
	}
	blockAlignedMaxt := head.MaxTime() - (head.MaxTime() % t.tsdbOpts.MaxBlockDuration)
	// Flush a well aligned TSDB block.
	if err := db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), blockAlignedMaxt-1)); err != nil {
		return err
	}
	// Flush the remainder of the head.
	return db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime()-1))
}

func (t *MultiTSDB) Close() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	merr := errutil.MultiError{}
	for id, tenant := range t.tenants {
		db := tenant.readyStorage().Get()
		if db == nil {
			level.Error(t.logger).Log("msg", "closing TSDB failed; not ready", "tenant", id)
			continue
		}
		level.Info(t.logger).Log("msg", "closing TSDB", "tenant", id)
		merr.Add(db.Close())
	}
	return merr.Err()
}

// Prune flushes and closes the TSDB for tenants that haven't received
// any new samples for longer than the TSDB retention period.
func (t *MultiTSDB) Prune(ctx context.Context) error {
	// Retention of 0 means infinite retention.
	if t.tsdbOpts.RetentionDuration == 0 {
		return nil
	}
	level.Info(t.logger).Log("msg", "Running pruning job")

	var (
		wg   sync.WaitGroup
		merr errutil.SyncMultiError

		prunedTenants []string
		pmtx          sync.Mutex

		tenants = make(map[tsdbID]*tenant)
	)

	t.mtx.RLock()
	maps.Copy(tenants, t.tenants)
	t.mtx.RUnlock()

	begin := time.Now()
	for id, tenantInstance := range tenants {
		wg.Add(1)
		go func(id tsdbID, tenantInstance *tenant) {
			defer wg.Done()

			pruned, err := t.pruneTSDB(ctx, t.tsdbLogger(id), tenantInstance, id)
			if err != nil {
				merr.Add(err)
				return
			}

			if pruned {
				pmtx.Lock()
				defer pmtx.Unlock()
				prunedTenants = append(prunedTenants, id.String())
			}
		}(id, tenantInstance)
	}
	wg.Wait()

	level.Info(t.logger).Log("msg", "Pruning job completed", "pruned_tenants_count", len(prunedTenants), "pruned_tenants", prunedTenants, "took_seconds", time.Since(begin).Seconds())

	return merr.Err()
}

// pruneTSDB removes a TSDB if its past the retention period.
// It compacts the TSDB head, sends all remaining blocks to S3 and removes the TSDB from disk.
func (t *MultiTSDB) pruneTSDB(ctx context.Context, logger log.Logger, tenantInstance *tenant, id tsdbID) (pruned bool, rerr error) {
	tenantTSDB := tenantInstance.readyStorage()
	if tenantTSDB == nil {
		return false, nil
	}

	tdb := tenantTSDB.Get()
	if tdb == nil {
		return false, nil
	}

	head := tdb.Head()
	if head.MaxTime() < 0 {
		return false, nil
	}

	sinceLastAppendMillis := time.Since(time.UnixMilli(head.MaxTime())).Milliseconds()
	compactThreshold := int64(1.5 * float64(t.tsdbOpts.MaxBlockDuration))
	if sinceLastAppendMillis <= compactThreshold {
		return false, nil
	}

	// Acquire a write lock and check that no writes have occurred in-between locks.
	tenantTSDB.mtx.Lock()
	defer tenantTSDB.mtx.Unlock()

	// Make sure the shipper is not running in parallel.
	tenantInstance.mtx.Lock()
	shipper := tenantInstance.ship
	tenantInstance.ship = nil
	tenantInstance.mtx.Unlock()

	defer func() {
		if pruned {
			return
		}
		// If the tenant was not pruned, re-enable the shipper.
		tenantInstance.mtx.Lock()
		tenantInstance.ship = shipper
		tenantInstance.mtx.Unlock()
	}()

	sinceLastAppendMillis = time.Since(time.UnixMilli(head.MaxTime())).Milliseconds()
	if sinceLastAppendMillis <= compactThreshold {
		return false, nil
	}

	level.Info(logger).Log("msg", "Compacting tenant")
	if err := t.flushHead(tdb); err != nil {
		return false, err
	}

	if sinceLastAppendMillis <= t.tsdbOpts.RetentionDuration {
		return false, nil
	}

	level.Info(logger).Log("msg", "Pruning tenant")
	if shipper != nil {
		// No other code can reach this shipper anymore so enable it again to be able to sync manually.
		uploaded, err := shipper.Sync(ctx)
		if err != nil {
			return false, err
		}

		if uploaded > 0 {
			level.Info(logger).Log("msg", "Uploaded head block")
		}
	}

	tenantInstance.setReadOnly(true)
	defer func() {
		if pruned {
			return
		}

		tenantInstance.setReadOnly(false)
	}()

	if err := tdb.Close(); err != nil {
		return false, err
	}

	if err := os.RemoveAll(tdb.Dir()); err != nil {
		return false, err
	}

	tenantInstance.mtx.Lock()
	tenantInstance.readyS.set(nil)
	tenantInstance.setComponents(nil, nil, nil, nil, nil)
	tenantInstance.mtx.Unlock()

	t.mtx.Lock()
	t.removeTenantUnlocked(id)
	t.mtx.Unlock()

	return true, nil
}

func (t *MultiTSDB) Sync(ctx context.Context) (int, error) {
	if t.bucket == nil {
		return 0, errors.New("bucket is not specified, Sync should not be invoked")
	}

	t.mtx.RLock()
	defer t.mtx.RUnlock()

	var (
		errmtx   = &sync.Mutex{}
		merr     = errutil.MultiError{}
		wg       = &sync.WaitGroup{}
		uploaded atomic.Int64
	)

	for id, tenant := range t.tenants {
		level.Debug(t.logger).Log("msg", "uploading block for tenant", "tenant", id)
		s := tenant.shipper()
		if s == nil {
			continue
		}
		wg.Go(func() {
			up, err := s.Sync(ctx)
			if err != nil {
				errmtx.Lock()
				merr.Add(errors.Wrap(err, "upload"))
				errmtx.Unlock()
			}
			uploaded.Add(int64(up))
		})
	}
	wg.Wait()
	return int(uploaded.Load()), merr.Err()
}

func (t *MultiTSDB) RemoveLockFilesIfAny() error {
	ids, err := t.discoverTSDBs()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	merr := errutil.MultiError{}
	for _, id := range ids {
		if err := os.Remove(filepath.Join(t.defaultTenantDataDir(id.dir()), "lock")); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			merr.Add(err)
			continue
		}
		level.Info(t.logger).Log("msg", "a leftover lockfile found and removed", "tenant", id)
	}
	return merr.Err()
}

// TSDBLocalClients should be used as read-only.
func (t *MultiTSDB) TSDBLocalClients() []store.Client {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.tsdbClients
}

// TSDBExemplars should be used as read-only.
func (t *MultiTSDB) TSDBExemplars() map[string]*exemplars.TSDB {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.exemplarClients
}

// TenantStats returns the stats of each TSDB of the given tenants (all tenants if none given). The
// stats of a per-replica TSDB are named like `tenant{replica_label="value"}`.
func (t *MultiTSDB) TenantStats(limit int, statsByLabelName string, tenantIDs ...string) []api.TenantStats {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		result = make([]api.TenantStats, 0, len(t.tenants))
	)
	for id, tenantInstance := range t.tenants {
		if len(tenantIDs) > 0 && !slices.Contains(tenantIDs, id.tenant) {
			continue
		}

		wg.Add(1)
		go func(id tsdbID, tenantInstance *tenant) {
			defer wg.Done()
			db := tenantInstance.readyS.Get()
			if db == nil {
				return
			}
			stats := db.Head().Stats(statsByLabelName, limit)

			mu.Lock()
			defer mu.Unlock()
			result = append(result, api.TenantStats{
				Tenant: id.String(),
				Stats:  stats,
			})
		}(id, tenantInstance)
	}
	wg.Wait()

	sort.Slice(result, func(i, j int) bool {
		return result[i].Tenant < result[j].Tenant
	})
	return result
}

func (t *MultiTSDB) startTSDB(logger log.Logger, id tsdbID, tenant *tenant) error {
	reg := prometheus.WrapRegistererWith(t.metricLabels(id), t.reg)
	reg = NewUnRegisterer(reg)

	lset, _ := t.externalLabels(id)
	dataDir := t.defaultTenantDataDir(id.dir())

	level.Info(logger).Log("msg", "opening TSDB")

	var expandedPostingsCache expandedpostingscache.ExpandedPostingsCache
	if t.headExpandedPostingsCacheSize > 0 || t.blockExpandedPostingsCacheSize > 0 {
		var expandedPostingsCacheMetrics = expandedpostingscache.NewPostingCacheMetrics(extprom.WrapRegistererWithPrefix("thanos_", reg))

		expandedPostingsCache = expandedpostingscache.NewBlocksPostingsForMatchersCache(expandedPostingsCacheMetrics, t.headExpandedPostingsCacheSize, t.blockExpandedPostingsCacheSize, 0)
	}

	opts := *t.tsdbOpts
	opts.BlocksToDelete = tenant.blocksToDelete
	opts.EnableDelayedCompaction = true
	opts.CompactionDelayMaxPercent = tsdb.DefaultCompactionDelayMaxPercent

	opts.BlockChunkQuerierFunc = func(b tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
		if expandedPostingsCache != nil {
			return expandedpostingscache.NewCachedBlockChunkQuerier(expandedPostingsCache, b, mint, maxt)
		}
		return tsdb.NewBlockChunkQuerier(b, mint, maxt)
	}
	if expandedPostingsCache != nil {
		opts.SeriesLifecycleCallback = expandedPostingsCache
	}
	tenant.blocksToDeleteFn = tsdb.DefaultBlocksToDelete

	// NOTE(GiedriusS): always set to false to properly handle OOO samples - OOO samples are written into the WBL
	// which gets later converted into a block. Without setting this flag to false, the block would get compacted
	// into other ones. This presents a race between compaction and the shipper (if it is configured to upload compacted blocks).
	// Hence, avoid this situation by disabling overlapping compaction. Vertical compaction must be enabled on the compactor.
	opts.EnableOverlappingCompaction = false

	// We don't do scrapes ourselves so this only gives us a performance penalty.
	opts.IsolationDisabled = true

	s, err := tsdb.Open(
		dataDir,
		logutil.GoKitLogToSlog(logger),
		reg,
		&opts,
		nil,
	)
	if err != nil {
		t.removeTenantLocked(id)
		return err
	}
	var ship *shipper.Shipper
	if t.bucket != nil {
		ship = shipper.New(
			t.bucket,
			dataDir,
			shipper.WithLogger(logger),
			shipper.WithRegisterer(reg),
			shipper.WithSource(metadata.ReceiveSource),
			shipper.WithHashFunc(t.hashFunc),
			shipper.WithMetaFileName(shipper.DefaultMetaFilename),
			shipper.WithLabels(func() labels.Labels { return lset }),
			shipper.WithAllowOutOfOrderUploads(t.allowOutOfOrderUpload),
			shipper.WithSkipCorruptedBlocks(t.skipCorruptedBlocks),
			shipper.WithUploadConcurrency(t.uploadConcurrency),
		)
	}
	var options []store.TSDBStoreOption
	if t.metricNameFilterEnabled {
		options = append(options, store.WithCuckooMetricNameStoreFilter())
	}
	if t.matcherCache != nil {
		options = append(options, store.WithMatcherCacheInstance(t.matcherCache))
	}
	tenant.set(store.NewTSDBStore(logger, s, component.Receive, lset, options...), s, ship, exemplars.NewTSDB(s, lset), reg.(*UnRegisterer))
	t.addTenantLocked(id, tenant) // need to update the client list once store is ready & client != nil
	level.Info(logger).Log("msg", "TSDB is now ready")
	return nil
}

func (t *MultiTSDB) defaultTenantDataDir(tenantID string) string {
	return path.Join(t.dataDir, tenantID)
}

func (t *MultiTSDB) getOrLoadTenant(id tsdbID) (*tenant, error) {
	// Fast path, as creating tenants is a very rare operation.
	t.mtx.RLock()
	tenant, exist := t.tenants[id]
	t.mtx.RUnlock()
	if exist {
		return tenant, t.initTSDBIfNeeded(id, tenant)
	}

	if err := id.validate(); err != nil {
		return nil, err
	}

	// Slow path needs to lock fully and attempt to read again to prevent race
	// conditions, where since the fast path was tried, there may have actually
	// been the same tenant inserted in the map.
	t.mtx.Lock()
	tenant, exist = t.tenants[id]
	if exist {
		t.mtx.Unlock()
		return tenant, t.initTSDBIfNeeded(id, tenant)
	}

	tenant = newTenant()
	t.addTenantUnlocked(id, tenant)
	t.mtx.Unlock()

	return tenant, t.initTSDBIfNeeded(id, tenant)
}

func (t *MultiTSDB) TenantAppendable(tenantID string) (Appendable, error) {
	tenant, err := t.getOrLoadTenant(tsdbID{tenant: tenantID})
	if err != nil {
		return nil, err
	}
	return tenant.readyStorage(), nil
}

// SeriesReplicaLabelName returns the series replica label name set by WithSeriesReplicaLabelName.
func (t *MultiTSDB) SeriesReplicaLabelName() string {
	return t.seriesReplicaLabelName
}

// TenantReplicaAppendable returns the Appendable of the per-replica TSDB of the tenant for the given
// value of the series replica label, or of the tenant's own TSDB if the value is empty. The caller
// removes the series replica label from the series it appends.
func (t *MultiTSDB) TenantReplicaAppendable(tenantID, replica string) (Appendable, error) {
	if t.seriesReplicaLabelName == "" {
		return nil, errors.New("no series replica label configured")
	}
	if replica == "" {
		return t.TenantAppendable(tenantID)
	}
	tenant, err := t.getOrLoadTenant(tsdbID{tenant: tenantID, replica: labels.Label{Name: t.seriesReplicaLabelName, Value: replica}})
	if err != nil {
		return nil, err
	}
	return tenant.readyStorage(), nil
}

func (t *MultiTSDB) SetHashringConfig(cfg []HashringConfig) error {
	t.hashringConfigsMtx.Lock()
	t.hashringConfigs = cfg
	t.hashringConfigsMtx.Unlock()

	t.mtx.RLock()
	tenants := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	// If a tenant's already existed in MultiTSDB, update the label set of its TSDBs
	// from the latest []HashringConfig, the same way as startTSDB does.
	for id, tenant := range tenants {
		if lset, inHashring := t.externalLabels(id); inHashring {
			tenant.setExtLabels(lset)
		}
	}

	return nil
}

// ErrNotReady is returned if the underlying storage is not ready yet.
var ErrNotReady = errors.New("TSDB not ready")

// ReadyStorage implements the Storage interface while allowing to set the actual
// storage at a later point in time.
// TODO: Replace this with upstream Prometheus implementation when it is exposed.
type ReadyStorage struct {
	mtx sync.RWMutex
	a   *adapter
}

// Set the storage.
func (s *ReadyStorage) Set(db *tsdb.DB) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.set(&adapter{db: db})
}

func (s *ReadyStorage) set(a *adapter) {
	s.a = a
}

// Get the storage.
func (s *ReadyStorage) Get() *tsdb.DB {
	if x := s.get(); x != nil {
		return x.db
	}
	return nil
}

func (s *ReadyStorage) get() *adapter {
	s.mtx.RLock()
	x := s.a
	s.mtx.RUnlock()
	return x
}

// StartTime implements the Storage interface.
func (s *ReadyStorage) StartTime() (int64, error) {
	return 0, errors.New("not implemented")
}

// Querier implements the Storage interface.
func (s *ReadyStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(mint, maxt)
	}
	return nil, ErrNotReady
}

// ExemplarQuerier implements the Storage interface.
func (s *ReadyStorage) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	if x := s.get(); x != nil {
		return x.ExemplarQuerier(ctx)
	}
	return nil, ErrNotReady
}

// Appender implements the Storage interface.
func (s *ReadyStorage) Appender(ctx context.Context) (storage.Appender, error) {
	if x := s.get(); x != nil {
		return x.Appender(ctx)
	}
	return nil, ErrNotReady
}

// Close implements the Storage interface.
func (s *ReadyStorage) Close() error {
	if x := s.Get(); x != nil {
		return x.Close()
	}
	return nil
}

// adapter implements a storage.Storage around TSDB.
type adapter struct {
	db *tsdb.DB
}

// StartTime implements the Storage interface.
func (a adapter) StartTime() (int64, error) {
	return 0, errors.New("not implemented")
}

func (a adapter) Querier(mint, maxt int64) (storage.Querier, error) {
	return a.db.Querier(mint, maxt)
}

func (a adapter) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return a.db.ExemplarQuerier(ctx)
}

// Appender returns a new appender against the storage.
func (a adapter) Appender(ctx context.Context) (storage.Appender, error) {
	return a.db.Appender(ctx), nil
}

// Close closes the storage and all its underlying resources.
func (a adapter) Close() error {
	return a.db.Close()
}

// UnRegisterer is a Prometheus registerer that
// ensures that collectors can be registered
// by unregistering already-registered collectors.
// FlushableStorage uses this registerer in order
// to not lose metric values between DB flushes.
//
// This type cannot embed the inner registerer, because Prometheus since
// v2.39.0 is wrapping the Registry with prometheus.WrapRegistererWithPrefix.
// This wrapper will call the Register function of the wrapped registerer.
// If UnRegisterer is the wrapped registerer, this would end up calling the
// inner registerer's Register, which doesn't implement the "unregister" logic
// that this type intends to use.
type UnRegisterer struct {
	innerReg prometheus.Registerer

	collectors []prometheus.Collector
}

func NewUnRegisterer(inner prometheus.Registerer) *UnRegisterer {
	return &UnRegisterer{innerReg: inner}
}

// UnregisterAll unregisters all collectors in a best-effort manner.
func (u *UnRegisterer) UnregisterAll() {
	for _, c := range u.collectors {
		u.innerReg.Unregister(c)
	}
}

// Register registers the given collector. If it's already registered, it will
// be unregistered and registered.
func (u *UnRegisterer) Register(c prometheus.Collector) error {
	if err := u.innerReg.Register(c); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if ok = u.innerReg.Unregister(c); !ok {
				panic("unable to unregister existing collector")
			}
			u.innerReg.MustRegister(c)

			u.collectors = append(u.collectors, c)
			return nil
		}
		return err
	}

	u.collectors = append(u.collectors, c)
	return nil
}

// Unregister unregisters the given collector.
func (u *UnRegisterer) Unregister(c prometheus.Collector) bool {
	return u.innerReg.Unregister(c)
}

// MustRegister registers the given collectors. It panics if an error happens.
// Note that if a collector is already registered it will be re-registered
// without panicking.
func (u *UnRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := u.Register(c); err != nil {
			panic(err)
		}
	}
	u.collectors = append(u.collectors, cs...)
}

// hashringExternalLabels returns the external labels of the tenant from the hashring configs.
// If one tenant appears in multiple hashring configs,
// only the external label set from the first hashring config is applied.
func (t *MultiTSDB) hashringExternalLabels(tenantID string) (labels.Labels, bool) {
	t.hashringConfigsMtx.RLock()
	defer t.hashringConfigsMtx.RUnlock()

	for _, hc := range t.hashringConfigs {
		if slices.Contains(hc.Tenants, tenantID) {
			return hc.ExternalLabels, true
		}
	}
	return labels.EmptyLabels(), false
}

// externalLabels returns the external labels of a TSDB: the receive labels, the tenant label and
// the tenant's hashring external labels (which the former two override) and, for a per-replica
// TSDB, the series replica label. It also tells whether the tenant is in a hashring config.
func (t *MultiTSDB) externalLabels(id tsdbID) (labels.Labels, bool) {
	lset := labelpb.ExtendSortedLabels(t.labels, labels.FromStrings(t.tenantLabelName, id.tenant))
	hashringLset, inHashring := t.hashringExternalLabels(id.tenant)
	if inHashring {
		lset = labelpb.ExtendSortedLabels(hashringLset, lset)
	}
	if id.isReplica() {
		lset = labelpb.ExtendSortedLabels(lset, labels.New(id.replica))
	}
	return lset, inHashring
}

// metricLabels returns the labels added to the metrics of a TSDB.
func (t *MultiTSDB) metricLabels(id tsdbID) prometheus.Labels {
	l := prometheus.Labels{"tenant": id.tenant}
	if t.seriesReplicaMetricLabel {
		l[seriesReplicaMetricLabel] = id.replica.Value
	}
	return l
}

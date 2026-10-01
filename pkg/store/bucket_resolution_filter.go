// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"cmp"
	"context"
	"maps"
	"math"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// resolutionFallbacks is what the store gateway does for the resolution
// filter (--min-block-resolution) beyond fetching metadata: it brings hidden
// finer blocks back while their covers cannot be used, and, if enabled, warns
// requests that ask for finer data than the minimum about the blocks still
// hidden.
type resolutionFallbacks struct {
	filter *block.ResolutionMetaFilter
	// following are the filters that run after the resolution filter in the
	// fetcher. They apply to the fallbacks too, the time partition in
	// particular.
	following []block.MetadataFilter
	warn      bool

	// verifiedMtx guards verified: the covers whose meta.json and index
	// objects were found complete in the bucket. Block objects are immutable,
	// so a cover is checked once for as long as it stays a cover.
	verifiedMtx sync.Mutex
	verified    map[ulid.ULID]struct{}

	// hidden is the latest sync's snapshot of the blocks the filter hid and
	// the store does not serve, cut to the store's time window. Requests read
	// it without a lock. Only kept with warnings enabled.
	hidden atomic.Pointer[hiddenResolution]

	warnings prometheus.Counter
}

// hiddenResolution is an immutable snapshot of the hidden blocks.
type hiddenResolution struct {
	floor  int64
	blocks []hiddenBlock // Ordered by minTime.
}

// hiddenBlock is the part of a hidden block within the store's time window.
type hiddenBlock struct {
	id               ulid.ULID
	minTime, maxTime int64 // Half-open, as in block metadata.
	labels           labels.Labels
}

// WithResolutionFilter makes the store bring back the finer blocks the
// resolution filter hid while their covers cannot be used. following are the
// filters that run after the resolution filter in the fetcher; they also
// constrain the restored blocks.
func WithResolutionFilter(f *block.ResolutionMetaFilter, following ...block.MetadataFilter) BucketStoreOption {
	return func(s *BucketStore) {
		r := s.resolutionOptions()
		r.filter, r.following = f, following
	}
}

// WithHiddenResolutionWarning makes a Series request asking for data finer than
// the resolution filter's minimum get a warning when its range overlaps blocks
// hidden behind coarser covers. The warning fails such a request under the
// strict partial response strategy. It needs WithResolutionFilter.
func WithHiddenResolutionWarning(enabled bool) BucketStoreOption {
	return func(s *BucketStore) {
		s.resolutionOptions().warn = enabled
	}
}

func (s *BucketStore) resolutionOptions() *resolutionFallbacks {
	if s.resolution == nil {
		s.resolution = &resolutionFallbacks{verified: map[ulid.ULID]struct{}{}}
	}
	return s.resolution
}

// initResolutionFallbacks completes the options once the store's registry is
// known.
func (s *BucketStore) initResolutionFallbacks() {
	if s.resolution == nil {
		return
	}
	if s.resolution.filter == nil {
		// Nothing is hidden without a resolution filter.
		s.resolution = nil
		return
	}
	s.resolution.warnings = promauto.With(s.reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_hidden_resolution_warnings_total",
		Help: "Total number of Series requests warned that blocks hidden behind coarser blocks by --min-block-resolution are missing from their answer.",
	})
}

// syncResolutionFallbacks runs after SyncBlocks added the blocks the fetcher
// returned, covers included, and before it drops the blocks no longer
// returned, so a finer block stays served until its cover is usable. It
// verifies the covers, restores the hidden blocks whose covers are not usable
// and adds them to metas so they are not dropped, and reports the blocks
// served below the minimum resolution.
//
// A fallback that fails to load is tolerated the way SyncBlocks tolerates any
// other block: the failure is logged and the sync goes on, and the next sync
// retries it.
func (s *BucketStore) syncResolutionFallbacks(ctx context.Context, metas map[ulid.ULID]*metadata.Meta) error {
	r := s.resolution
	if r == nil {
		return nil
	}
	s.verifyCovers(ctx, r.filter.Covers(), metas)

	fallbacks := r.filter.FallbacksFor(metas, s.usableCover)
	if len(fallbacks) > 0 {
		// The counts of the filters re-run on fallback metadata are not exported:
		// the fetcher's metrics describe its own pass, and the reporter the
		// served fallbacks.
		discarded := promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{Name: "fallback_filter", Help: "Counts of the metadata filters re-run on fallback blocks; unregistered."}, []string{"state"})
		for _, filter := range r.following {
			if err := filter.Filter(ctx, fallbacks, discarded, discarded); err != nil {
				return errors.Wrap(err, "filter resolution fallbacks")
			}
		}
		maps.Copy(metas, fallbacks)
		s.loadBlocks(ctx, fallbacks)
	}
	return r.filter.Reporter().Filter(ctx, metas, nil, nil)
}

// verifyCovers checks the covers this store serves that were not verified
// yet, with the store's block sync concurrency. A cover that fails is dropped
// from the store, so queries read the finer blocks it would hide, which the
// fallbacks restore. The sync that re-adds it checks it again.
//
// The check is cheap on purpose: the meta.json and index objects exist and
// are not empty, and the index has the size meta.json records. Reading the
// cover's index header instead would build it even with lazy index-header
// reading or downloading, on every block that hides another. A cover that
// passes but whose index header cannot be built fails when it is added, as
// with eager loading, or when a query loads it lazily; the latter fails the
// query rather than hiding its data.
func (s *BucketStore) verifyCovers(ctx context.Context, covers []*metadata.Meta, metas map[ulid.ULID]*metadata.Meta) {
	r := s.resolution
	current := make(map[ulid.ULID]struct{}, len(covers))
	var unverified []*metadata.Meta
	r.verifiedMtx.Lock()
	for _, m := range covers {
		current[m.ULID] = struct{}{}
		if _, ok := r.verified[m.ULID]; !ok {
			unverified = append(unverified, m)
		}
	}
	// Forget the blocks that are no longer covers.
	for id := range r.verified {
		if _, ok := current[id]; !ok {
			delete(r.verified, id)
		}
	}
	r.verifiedMtx.Unlock()
	// A cover not served here needs no check: it is trusted as another
	// store's when out of view, and unusable when it failed to load.
	unverified = slices.DeleteFunc(unverified, func(m *metadata.Meta) bool {
		_, inView := metas[m.ULID]
		return !inView || s.getBlock(m.ULID) == nil
	})

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.blockSyncConcurrency)
	for _, m := range unverified {
		g.Go(func() error {
			if err := s.verifyCoverObjects(gctx, m); err != nil {
				level.Warn(s.logger).Log("msg", "cover of a hidden block is incomplete in the bucket; serving the finer blocks instead", "block", m.ULID, "err", err)
				if err := s.removeBlock(m.ULID); err != nil {
					level.Warn(s.logger).Log("msg", "drop of incomplete cover failed", "block", m.ULID, "err", err)
				}
				return nil
			}
			r.verifiedMtx.Lock()
			r.verified[m.ULID] = struct{}{}
			r.verifiedMtx.Unlock()
			return nil
		})
	}
	_ = g.Wait()
}

// verifyCoverObjects checks the objects a cover is read from without
// downloading them.
func (s *BucketStore) verifyCoverObjects(ctx context.Context, m *metadata.Meta) error {
	dir := m.ULID.String()
	attrs, err := s.bkt.Attributes(ctx, path.Join(dir, metadata.MetaFilename))
	if err != nil {
		return errors.Wrap(err, "meta.json")
	}
	if attrs.Size <= 0 {
		return errors.New("meta.json is empty")
	}
	attrs, err = s.bkt.Attributes(ctx, path.Join(dir, block.IndexFilename))
	if err != nil {
		return errors.Wrap(err, "index")
	}
	if attrs.Size <= 0 {
		return errors.New("index is empty")
	}
	for _, f := range m.Thanos.Files {
		if f.RelPath == block.IndexFilename && f.SizeBytes > 0 && f.SizeBytes != attrs.Size {
			return errors.Errorf("index has %d bytes, meta.json records %d", attrs.Size, f.SizeBytes)
		}
	}
	return nil
}

// usableCover reports whether a cover is served and was verified.
func (s *BucketStore) usableCover(m *metadata.Meta) bool {
	if s.getBlock(m.ULID) == nil {
		return false
	}
	r := s.resolution
	r.verifiedMtx.Lock()
	defer r.verifiedMtx.Unlock()
	_, ok := r.verified[m.ULID]
	return ok
}

// publishHiddenResolution publishes the snapshot of the hidden blocks this
// store does not serve, cut to its time window, for the warnings to read.
func (s *BucketStore) publishHiddenResolution() {
	r := s.resolution
	if r == nil || !r.warn {
		return
	}
	// The window's max time is inclusive, block max times are not.
	mint, maxt := s.limitMinTime(math.MinInt64), s.limitMaxTime(math.MaxInt64-1)+1
	hidden := r.filter.HiddenBlocks()
	snapshot := &hiddenResolution{floor: r.filter.MinimumResolution()}
	s.mtx.RLock()
	for _, m := range hidden {
		if _, served := s.blocks[m.ULID]; served {
			continue
		}
		b := hiddenBlock{id: m.ULID, minTime: max(m.MinTime, mint), maxTime: min(m.MaxTime, maxt), labels: labels.FromMap(m.Thanos.Labels)}
		if b.minTime >= b.maxTime {
			continue
		}
		snapshot.blocks = append(snapshot.blocks, b)
	}
	s.mtx.RUnlock()
	slices.SortFunc(snapshot.blocks, func(a, b hiddenBlock) int { return cmp.Compare(a.minTime, b.minTime) })
	r.hidden.Store(snapshot)
}

// hiddenResolutionWarning returns the warning for a request asking for data
// finer than the minimum resolution whose range overlaps blocks the store
// hides behind covers at the minimum, or nil. The block sets never substitute
// coarser blocks for finer ones, so such a request gets nothing for those
// ranges. The store cannot tell whether a hidden block holds any of the
// requested series without reading it, so it judges by time range, external
// labels and block matchers only.
func (s *BucketStore) hiddenResolutionWarning(req *storepb.SeriesRequest, matchers, blockMatchers []*labels.Matcher) error {
	r := s.resolution
	if r == nil || !r.warn {
		return nil
	}
	snapshot := r.hidden.Load()
	if snapshot == nil || req.MaxResolutionWindow >= snapshot.floor {
		return nil
	}
	missing := 0
	for _, b := range snapshot.blocks {
		if b.minTime > req.MaxTime {
			break
		}
		if b.maxTime <= req.MinTime || !b.matches(matchers, blockMatchers) {
			continue
		}
		missing++
	}
	if missing == 0 {
		return nil
	}
	r.warnings.Inc()
	return errors.Errorf(
		"this store hides finer blocks where blocks downsampled to %s cover them (--min-block-resolution), but the request asks for finer data (max_source_resolution=%s); "+
			"%d hidden block(s) overlap the requested range and labels and are missing from this answer unless another store serves them; "+
			"raise the query's max_source_resolution, enable --query.auto-downsampling on the querier, or route the query to a store serving finer blocks",
		time.Duration(snapshot.floor)*time.Millisecond,
		time.Duration(req.MaxResolutionWindow)*time.Millisecond,
		missing,
	)
}

// matches reports whether the hidden block could answer the request: its
// external labels agree with every matcher naming one of them, as a block
// set's labels do, and it passes the request's block matchers, which may name
// the block ID as well.
func (b hiddenBlock) matches(matchers, blockMatchers []*labels.Matcher) bool {
	for _, matcher := range matchers {
		if v := b.labels.Get(matcher.Name); v != "" && !matcher.Matches(v) {
			return false
		}
	}
	for _, matcher := range blockMatchers {
		v := b.labels.Get(matcher.Name)
		if matcher.Name == block.BlockIDLabel {
			v = b.id.String()
		}
		if !matcher.Matches(v) {
			return false
		}
	}
	return true
}

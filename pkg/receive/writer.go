// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender(ctx context.Context) (storage.Appender, error)
}

type TenantStorage interface {
	TenantAppendable(string) (Appendable, error)
}

// seriesReplicaStorage is a TenantStorage that keeps the series of each value of a series replica
// label in a separate TSDB of the tenant, see WithSeriesReplicaLabelName.
type seriesReplicaStorage interface {
	// SeriesReplicaLabelName returns the series replica label name, empty if there is none.
	SeriesReplicaLabelName() string
	// TenantReplicaAppendable returns the Appendable for the series of the tenant that carried the
	// series replica label with the given value; the label is removed from the appended series.
	TenantReplicaAppendable(tenantID, replica string) (Appendable, error)
}

// seriesReplicaLabelName returns the series replica label name of the storage, empty if it has none.
func seriesReplicaLabelName(s TenantStorage) string {
	if rs, ok := s.(seriesReplicaStorage); ok {
		return rs.SeriesReplicaLabelName()
	}
	return ""
}

// tenantAppender is an appender of one TSDB of a tenant.
type tenantAppender struct {
	app    storage.Appender
	getRef storage.GetRef
}

// tenantAppenders opens, on first use, an appender for each TSDB of a tenant that a write request
// touches: the tenant's own TSDB and, with a series replica label, its per-replica TSDBs.
type tenantAppenders struct {
	ctx            context.Context
	storage        TenantStorage
	tenantID       string
	tLogger        log.Logger
	tooFarInFuture int64

	tenant   *tenantAppender
	replicas map[string]*tenantAppender
}

func newTenantAppenders(ctx context.Context, s TenantStorage, tenantID string, tLogger log.Logger, tooFarInFuture int64) *tenantAppenders {
	return &tenantAppenders{ctx: ctx, storage: s, tenantID: tenantID, tLogger: tLogger, tooFarInFuture: tooFarInFuture}
}

// get returns the appender for series with the given series replica label value, or for series
// without the label if the value is empty.
func (a *tenantAppenders) get(replica string) (*tenantAppender, error) {
	if replica == "" && a.tenant != nil {
		return a.tenant, nil
	}
	if ta, ok := a.replicas[replica]; ok {
		return ta, nil
	}

	var (
		s   Appendable
		err error
	)
	if replica == "" {
		s, err = a.storage.TenantAppendable(a.tenantID)
	} else {
		// The value can point into the request buffer, which is reused.
		replica = strings.Clone(replica)
		s, err = a.storage.(seriesReplicaStorage).TenantReplicaAppendable(a.tenantID, replica)
	}
	if err != nil {
		return nil, errors.Wrap(err, "get tenant appendable")
	}

	app, err := s.Appender(a.ctx)
	if err == tsdb.ErrNotReady {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, "get appender")
	}
	ta := &tenantAppender{
		app: &ReceiveAppender{
			tLogger:        a.tLogger,
			tooFarInFuture: a.tooFarInFuture,
			Appender:       app,
		},
		getRef: app.(storage.GetRef),
	}

	if replica == "" {
		a.tenant = ta
	} else {
		if a.replicas == nil {
			a.replicas = map[string]*tenantAppender{}
		}
		a.replicas[replica] = ta
	}
	return ta, nil
}

func (a *tenantAppenders) all() []*tenantAppender {
	all := make([]*tenantAppender, 0, 1+len(a.replicas))
	if a.tenant != nil {
		all = append(all, a.tenant)
	}
	for _, ta := range a.replicas {
		all = append(all, ta)
	}
	return all
}

// commit commits all opened appenders and adds their errors to errs.
func (a *tenantAppenders) commit(errs *writeErrors) {
	for _, ta := range a.all() {
		if err := ta.app.Commit(); err != nil {
			errs.Add(errors.Wrap(err, "commit samples"))
		}
	}
}

// rollback rolls back all opened appenders.
func (a *tenantAppenders) rollback() {
	for _, ta := range a.all() {
		if err := ta.app.Rollback(); err != nil {
			level.Warn(a.tLogger).Log("msg", "rolling back appender failed", "err", err)
		}
	}
}

// withoutZLabel returns the labels without the non-empty label of the given name, and that label's
// value. It returns the labels unchanged and an empty value if there is no such label. It never
// modifies lbls, which can be shared with requests forwarded to other receivers.
func withoutZLabel(lbls []labelpb.ZLabel, name string) ([]labelpb.ZLabel, string) {
	for i, l := range lbls {
		if l.Name != name {
			continue
		}
		if l.Value == "" {
			break
		}
		out := make([]labelpb.ZLabel, 0, len(lbls)-1)
		out = append(out, lbls[:i]...)
		return append(out, lbls[i+1:]...), l.Value
	}
	return lbls, ""
}

// Wraps storage.Appender to add validation and logging.
type ReceiveAppender struct {
	tLogger        log.Logger
	tooFarInFuture int64 // Unit: nanoseconds
	storage.Appender
}

func (ra *ReceiveAppender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if ra.tooFarInFuture > 0 {
		tooFar := model.Now().Add(time.Duration(ra.tooFarInFuture))
		if tooFar.Before(model.Time(t)) {
			level.Warn(ra.tLogger).Log("msg", "block metric too far in the future", "lset", lset,
				"timestamp", t, "bound", tooFar)
			// now + tooFarInFutureTimeWindow < sample timestamp
			return 0, storage.ErrOutOfBounds
		}
	}
	return ra.Appender.Append(ref, lset, t, v)
}

type WriterOptions struct {
	Intern                   bool
	TooFarInFutureTimeWindow int64 // Unit: nanoseconds
}

type Writer struct {
	logger    log.Logger
	multiTSDB TenantStorage
	opts      *WriterOptions

	seriesReplicaLabel string
}

// NewWriter returns a Writer to the tenants' TSDBs. If multiTSDB has a series replica label (see
// WithSeriesReplicaLabelName), the Writer removes it from series and writes them to the per-replica TSDBs.
func NewWriter(logger log.Logger, multiTSDB TenantStorage, opts *WriterOptions) *Writer {
	if opts == nil {
		opts = &WriterOptions{}
	}
	return &Writer{
		logger:             logger,
		multiTSDB:          multiTSDB,
		opts:               opts,
		seriesReplicaLabel: seriesReplicaLabelName(multiTSDB),
	}
}

func (r *Writer) Write(ctx context.Context, tenantID string, wreq []prompb.TimeSeries) error {
	tLogger := log.With(r.logger, "tenant", tenantID)

	apps := newTenantAppenders(ctx, r.multiTSDB, tenantID, tLogger, r.opts.TooFarInFutureTimeWindow)
	var (
		ref          storage.SeriesRef
		errorTracker writeErrorTracker
	)

	for _, t := range wreq {
		// Check if time series labels are valid. If not, skip the time series
		// and report the error.
		if err := labelpb.ValidateLabels(t.Labels); err != nil {
			lset := &labelpb.ZLabelSet{Labels: t.Labels}
			errorTracker.addLabelsError(err, lset, tLogger)
			continue
		}

		var replica string
		if r.seriesReplicaLabel != "" {
			lbls := t.Labels
			if t.Labels, replica = withoutZLabel(lbls, r.seriesReplicaLabel); len(t.Labels) == 0 {
				errorTracker.addLabelsError(labelpb.ErrEmptyLabels, &labelpb.ZLabelSet{Labels: lbls}, tLogger)
				continue
			}
		}
		ta, err := apps.get(replica)
		if err != nil {
			apps.rollback()
			return err
		}
		app := ta.app

		lset := labelpb.ZLabelsToPromLabels(t.Labels)

		// Check if the TSDB has cached reference for those labels.
		ref, lset = ta.getRef.GetRef(lset, lset.Hash())
		if ref == 0 {
			// If not, copy labels, as TSDB will hold those strings long term. Given no
			// copy unmarshal we don't want to keep memory for whole protobuf, only for labels.
			labelpb.ReAllocZLabelsStrings(&t.Labels, r.opts.Intern)
			lset = labelpb.ZLabelsToPromLabels(t.Labels)
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range t.Samples {
			ref, err = app.Append(ref, lset, s.Timestamp, s.Value)
			errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
		}

		for _, hp := range t.Histograms {
			var (
				h  *histogram.Histogram
				fh *histogram.FloatHistogram
			)

			if hp.IsFloatHistogram() {
				fh = prompb.FloatHistogramProtoToFloatHistogram(hp)
			} else {
				h = prompb.HistogramProtoToHistogram(hp)
			}

			ref, err = app.AppendHistogram(ref, lset, hp.Timestamp, h, fh)
			errorTracker.addHistogramError(err, tLogger, lset, hp.Timestamp)
		}

		// Current implementation of app.AppendExemplar doesn't create a new series, so it must be already present.
		// We drop the exemplars in case the series doesn't exist.
		if ref != 0 && len(t.Exemplars) > 0 {
			for _, ex := range t.Exemplars {
				exLset := labelpb.ZLabelsToPromLabels(ex.Labels)
				exLogger := log.With(tLogger, "exemplarLset", exLset, "exemplar", ex.String())

				if _, err = app.AppendExemplar(ref, lset, exemplar.Exemplar{
					Labels: exLset,
					Value:  ex.Value,
					Ts:     ex.Timestamp,
					HasTs:  true,
				}); err != nil {
					errorTracker.addExemplarError(err, exLogger)
				}
			}
		}
	}

	errs := errorTracker.collectErrors(tLogger)
	apps.commit(&errs)
	return errs.ErrOrNil()
}

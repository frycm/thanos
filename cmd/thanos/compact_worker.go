// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	"github.com/thanos-io/thanos/pkg/compact/distributed"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/prober"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/strutil"
)

// runCompactWorker runs the compactor in worker mode: it asks a manager for a
// task, executes it, reports the outcome, and repeats.
//
// A worker owns nothing in the bucket. It never plans, never garbage collects,
// never applies retention and never marks a source block for deletion; all of
// that stays with the manager, which is the single writer for its shard. The
// only thing a worker writes is the block it was asked to produce, and it
// re-checks that it still owns the task immediately before doing so.
func runCompactWorker(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	component component.Component,
	conf compactConfig,
) error {
	if conf.workerManagerAddress == "" {
		return errors.New("--compact.worker.manager-address is required in worker mode")
	}
	if conf.managerJournalID == "" {
		return errors.New("--compact.manager.journal-id is required in worker mode and has to match the manager's")
	}

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(component, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, component, httpProbe,
		httpserver.WithListen(conf.http.bindAddress),
		httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
		httpserver.WithTLSConfig(conf.http.tlsConfig),
	)
	g.Add(func() error {
		statusProber.Healthy()
		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)
		srv.Shutdown(err)
	})

	confContentYaml, err := conf.objStore.Content()
	if err != nil {
		return err
	}
	bkt, err := client.NewBucket(logger, confContentYaml, component.String(), nil)
	if err != nil {
		return err
	}
	insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

	ctx, cancel := context.WithCancel(context.Background())

	levels, err := compactions.levels(conf.maxCompactionLevel)
	if err != nil {
		cancel()
		return errors.Wrap(err, "get compaction levels")
	}

	mergeFunc, err := dedupFuncFor(conf, conf.dedupReplicaLabels)
	if err != nil {
		cancel()
		return err
	}

	comp, err := tsdb.NewLeveledCompactor(ctx, reg, logutil.GoKitLogToSlog(logger), levels, downsample.NewPool(), mergeFunc)
	if err != nil {
		cancel()
		return errors.Wrap(err, "create compactor")
	}

	workerDir := path.Join(conf.dataDir, "compact")
	if err := os.MkdirAll(workerDir, os.ModePerm); err != nil {
		cancel()
		return errors.Wrap(err, "create working directory")
	}

	dnsProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_compact_worker_manager_", reg),
		dns.ResolverType(conf.dnsSDResolver),
	)
	client := distributed.NewHTTPClient(logger, dnsProvider, conf.workerManagerAddress, 0)

	worker, err := distributed.NewWorker(logger, insBkt, client, comp, reg, distributed.WorkerConfig{
		WorkerID:           conf.workerID,
		JournalID:          conf.managerJournalID,
		DedupFunc:          conf.dedupFunc,
		DedupReplicaLabels: strutil.ParseFlagLabels(conf.dedupReplicaLabels),
		DataDir:            workerDir,
		PollInterval:       conf.workerPollInterval,
		HeartbeatInterval:  conf.workerHeartbeatInterval,
	})
	if err != nil {
		cancel()
		return errors.Wrap(err, "create compaction worker")
	}

	g.Add(func() error {
		defer func() {
			if err := os.RemoveAll(workerDir); err != nil {
				level.Error(logger).Log("msg", "could not clean up the working directory", "dir", workerDir, "err", err)
			}
		}()

		statusProber.Ready()
		level.Info(logger).Log("msg", "starting compact worker", "manager", conf.workerManagerAddress)
		if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	}, func(error) {
		cancel()
	})

	return nil
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	"github.com/thanos-io/thanos/pkg/compact/distributed"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
)

type bucketRollbackConfig struct {
	journalID             string
	allJournals           bool
	dryRun                bool
	force                 bool
	allowUnreadableBlocks bool
	livenessWindow        time.Duration
}

func (tbc *bucketRollbackConfig) registerBucketRollbackFlag(cmd extkingpin.FlagClause) *bucketRollbackConfig {
	cmd.Flag("journal-id", "Undo the work of the manager with this journal ID (its --compact.manager.journal-id). Required unless --all-journals is given.").
		StringVar(&tbc.journalID)
	cmd.Flag("all-journals", "Undo the work of every manager that ever wrote to the bucket. In a bucket shared by several shards this is every shard's trial at once; every one of their managers has to be stopped.").
		Default("false").BoolVar(&tbc.allJournals)
	cmd.Flag("dry-run", "Only print what would be done. Pass --no-dry-run to actually delete and restore blocks.").
		Default("true").BoolVar(&tbc.dryRun)
	cmd.Flag("manager-liveness-window", "A journal written within this window is treated as belonging to a manager that may still be running, and the rollback is refused. A running manager writes its journal at least once per --compact.manager.lease-ttl even when idle, so keep this at a few times the lease TTL.").
		Default("15m").DurationVar(&tbc.livenessWindow)
	cmd.Flag("force", "Apply even though a journal in scope was written within --manager-liveness-window. Only after confirming the manager and every worker are stopped: a rollback racing a running manager can leave the bucket with neither the sources nor their replacement.").
		Default("false").BoolVar(&tbc.force)
	cmd.Flag("allow-unreadable-blocks", "Plan even though the metadata of some blocks cannot be read. Only when those blocks are known to be unrelated to the distributed compactor's work, such as the remains of aborted uploads: a marked source among them would not be restored.").
		Default("false").BoolVar(&tbc.allowUnreadableBlocks)
	return tbc
}

func registerBucketRollbackDistributedCompaction(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("rollback-distributed-compaction",
		"Experimental. Undo what the distributed compactor (--compact.mode=manager/worker) did to the bucket: "+
			"restore the recorded sources, including garbage-collection marks, then delete the blocks its workers produced, "+
			"returning the bucket to the state the standalone compactor left behind. "+
			"Stop the manager and every worker first and wait for in-flight tasks to end; the command refuses to apply while a journal in scope looks alive. "+
			"Run it before the compactor's --delete-delay has passed for the oldest marks, since a block that was physically deleted cannot be restored. "+
			"Nothing is changed unless --no-dry-run is given.")

	tbc := &bucketRollbackConfig{}
	tbc.registerBucketRollbackFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		if tbc.journalID == "" && !tbc.allJournals {
			return errors.New("--journal-id is required; pass --all-journals to undo the work of every manager in the bucket")
		}
		if tbc.journalID != "" && tbc.allJournals {
			return errors.New("--journal-id and --all-journals are mutually exclusive")
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, "rollback-distributed-compaction", nil)
		if err != nil {
			return err
		}
		insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer cancel()
			return runBucketRollback(ctx, logger, insBkt, tbc)
		}, func(error) {
			cancel()
		})
		return nil
	})
}

func runBucketRollback(ctx context.Context, logger log.Logger, bkt objstore.InstrumentedBucket, tbc *bucketRollbackConfig) error {
	plan, err := distributed.PlanRollback(ctx, logger, bkt, distributed.RollbackOptions{
		JournalID:             tbc.journalID,
		AllJournals:           tbc.allJournals,
		AllowUnreadableBlocks: tbc.allowUnreadableBlocks,
	})
	if err != nil {
		return err
	}

	scope := "journal " + tbc.journalID
	if tbc.allJournals {
		scope = "every manager"
	}
	fmt.Fprintf(os.Stdout, "Rollback of the distributed compactor's work for %s:\n", scope)
	fmt.Fprintf(os.Stdout, "  %d block(s) produced by workers, to be deleted\n", len(plan.Produced))
	for _, id := range plan.Produced {
		fmt.Fprintf(os.Stdout, "    delete  %s\n", id)
	}
	fmt.Fprintf(os.Stdout, "  %d source block(s) marked by the manager or garbage collection, to be restored\n", len(plan.Restore))
	for _, id := range plan.Restore {
		fmt.Fprintf(os.Stdout, "    restore %s\n", id)
	}
	if len(plan.Unreadable) > 0 {
		fmt.Fprintf(os.Stdout, "  %d block(s) with unreadable metadata were left out of the plan, as allowed\n", len(plan.Unreadable))
		for _, id := range plan.Unreadable {
			fmt.Fprintf(os.Stdout, "    skipped %s\n", id)
		}
	}

	// A running manager keeps its journal fresh, so a journal written within
	// the window may belong to a manager that is still running. A rollback
	// racing it can delete a replacement the manager then re-marks the sources
	// of, leaving the bucket with neither.
	if active := plan.RecentlyActive(tbc.livenessWindow, time.Now()); len(active) > 0 {
		msg := fmt.Sprintf("journal(s) %s written within the last %s; the manager may still be running",
			strings.Join(active, ", "), tbc.livenessWindow)
		switch {
		case tbc.dryRun:
			fmt.Fprintf(os.Stdout, "WARNING: %s. Stop it and every worker before applying.\n", msg)
		case !tbc.force:
			return errors.Errorf("%s. Stop it and every worker, wait for in-flight tasks to end, and retry; "+
				"pass --force only once that is confirmed", msg)
		default:
			fmt.Fprintf(os.Stdout, "WARNING: %s; applying anyway because --force was given.\n", msg)
		}
	}

	if tbc.dryRun {
		fmt.Fprintln(os.Stdout, "Dry run: nothing was changed. Pass --no-dry-run to apply.")
		return nil
	}
	if len(plan.Produced) == 0 && len(plan.Restore) == 0 {
		fmt.Fprintln(os.Stdout, "Nothing to do.")
		return nil
	}

	if err := plan.Apply(ctx, logger, bkt); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Done: deleted %d block(s), restored %d block(s).\n", len(plan.Produced), len(plan.Restore))
	for id := range plan.JournalsUpdatedAt {
		fmt.Fprintf(os.Stdout, "The journal at %s was left in place; remove it before the next trial.\n", distributed.JournalPath(id))
	}
	return nil
}

// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// scenario is one fault to survive. Every scenario starts from the same
// uploaded corpus and ends by judging the bucket, most of them against the
// standalone compactor's result for the same blocks.
type scenario struct {
	name string
	// tenants overrides the corpus; nil means haCorpus.
	tenants []tenantSpec
	// conf overrides the manager configuration; the zero value means the
	// deployment's own (penalty deduplication over the receive replica).
	conf func(nodeConfig) nodeConfig
	run  func(t *testing.T, s *scenarioRun, want *bucketDump, input *bucketDump)
}

// converged runs the manager to convergence and asserts the common outcome:
// the bucket holds what standalone would have produced, nothing overlaps, no
// task is live, and every worker-produced block can still be rolled back.
func converged(t *testing.T, s *scenarioRun, want *bucketDump) *bucketDump {
	t.Helper()
	res := s.converge(90 * time.Second)
	testutil.Assert(t, !res.halted, "the manager halted: %v", res.lastErr)
	got := dumpBucket(t, s.shared)
	if !sameContent(t, want, got, "after convergence") {
		var entries []string
		for _, e := range s.journal().Tasks {
			entries = append(entries, fmt.Sprintf("%s %s attempts=%d gen=%d sources=%v outputs=%v err=%v",
				e.Task.Type, e.State, e.Attempts, e.Task.Generation, e.Task.SourceBlocks, e.Outputs, e.LastError))
		}
		t.Fatalf("journal after %d passes:\n  %s", res.iterations, strings.Join(entries, "\n  "))
	}
	got.assertNoOverlaps(t)
	got.assertProvenanceIntact(t, s.shared)
	testutil.Equals(t, 0, len(s.tasksInState(StatePending))+len(s.tasksInState(StateLeased)), "live tasks left in the journal")
	return got
}

// unparkAll writes an unpark marker for every parked task, as an operator
// would after fixing what parked them.
func unparkAll(t *testing.T, s *scenarioRun) int {
	t.Helper()
	n := 0
	for _, e := range s.journal().Tasks {
		if !e.State.Parked() {
			continue
		}
		testutil.Ok(t, s.shared.Upload(context.Background(), UnparkPath(s.conf.journalID, e.Task.ID), strings.NewReader("")))
		n++
	}
	return n
}

func scenarios() []scenario {
	return []scenario{
		{
			name: "golden_path",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				s.startWorker("w1")
				s.startWorker("w2")
				converged(t, s, want)
				testutil.Equals(t, 0, len(s.tasksInState(StateFailed))+len(s.tasksInState(StateAbandoned)))
			},
		},
		{
			// With one plan in flight per group the manager asks the same
			// planner in the same order as standalone does, so not only the
			// content but the block layout has to match.
			name: "single_inflight_matches_layout",
			conf: func(c nodeConfig) nodeConfig { c.maxInflight = 1; return c },
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				s.startWorker("w1")
				got := converged(t, s, want)
				testutil.Equals(t, want.layout(), got.layout())
			},
		},
		{
			name: "worker_crash_mid_task",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				w1 := s.startWorker("w1")
				_ = gateChunks(w1.testWorker) // w1 hangs mid download and never finishes.
				s.inject(func() {
					s.waitLeasedBy("w1")
					w1.crash()
					<-w1.done
					s.startWorker("w2")
				})
				converged(t, s, want)
				var reassigned bool
				for _, e := range s.tasksInState(StateCompleted) {
					if e.Attempts > 1 {
						reassigned = true
					}
				}
				testutil.Assert(t, reassigned, "no task records the lease expiry and reassignment")
				testutil.Equals(t, 0, len(s.tasksInState(StateAbandoned)))
			},
		},
		{
			name: "manager_restart_voids_old_leases",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				w1 := s.startWorker("w1")
				release := gateChunks(w1.testWorker)
				s.inject(func() {
					s.waitLeasedBy("w1")
					oldGen := s.currentManager().sched.Generation()
					n := s.replaceManager(s.conf)
					testutil.Assert(t, n.sched.Generation() > oldGen, "the new manager must bump the generation")
					release()
					s.startWorker("w2")
				})
				converged(t, s, want)
				testutil.Assert(t, counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeAbortedOwnershipLost)) >= 1,
					"the old manager's worker must discard its work on the takeover")
			},
		},
		{
			name: "two_managers_one_shard",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				w1 := s.startWorker("w1")
				release := gateChunks(w1.testWorker)
				first := s.currentManager()
				done := make(chan convergeResult, 1)
				go func() { done <- s.converge(90 * time.Second) }()

				s.waitLeasedBy("w1")
				// A second manager starts on the same journal by mistake. The
				// workers' URL now points at it; the first keeps running.
				second := newNode(t, s.shared, s.handler, s.conf)
				release()

				res := <-done
				testutil.Assert(t, res.halted, "the first manager must halt when another takes its journal: %v", res.lastErr)
				_ = first

				s.mtx.Lock()
				s.manager = second
				s.mtx.Unlock()
				s.startWorker("w2")
				converged(t, s, want)
			},
		},
		{
			name: "journal_unreachable_for_one_worker",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				w1 := s.startWorker("w1")
				release := gateChunks(w1.testWorker)
				s.inject(func() {
					s.waitLeasedBy("w1")
					// The finished work must be discarded, not uploaded, when
					// the journal cannot confirm ownership.
					w1.bkt.setOnGet(func(_ context.Context, name string) error {
						if strings.HasPrefix(name, JournalPrefix) {
							return errors.New("journal unreachable")
						}
						return nil
					})
					release()
					s.waitFor("w1 to abort on the unreachable journal", func() bool {
						return counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeAbortedStoreUnreachable)) >= 1
					})
					w1.bkt.setOnGet(nil)
				})
				converged(t, s, want)
				testutil.Equals(t, 0, len(s.tasksInState(StateFailed)), "an outage must not consume attempts")
			},
		},
		{
			name: "object_store_outage",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				w1 := s.startWorker("w1")
				w2 := s.startWorker("w2")
				s.inject(func() {
					s.waitFor("work to be in flight", func() bool { return len(s.tasksInState(StateLeased)) > 0 })
					outage := func(_ context.Context, _ string) error { return errors.New("object store down") }
					views := []*hookBucket{s.currentManager().bkt, w1.bkt, w2.bkt}
					for _, b := range views {
						b.setOnGet(outage)
						b.setOnUpload(outage)
					}
					time.Sleep(s.conf.leaseTTL * 3)
					for _, b := range views {
						b.setOnGet(nil)
						b.setOnUpload(nil)
					}
				})
				converged(t, s, want)
			},
		},
		{
			name: "worker_on_another_journal_is_refused",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				stray := s.startWorker("stray", workerOpts{journalID: "another-shard", dedupFunc: s.conf.dedupFunc, dedupReplicaLabels: s.conf.dedupReplicaLabels})
				s.startWorker("w1")
				converged(t, s, want)
				testutil.Equals(t, 0.0, counterTotal(t, stray.reg, "thanos_compact_worker_tasks_total"), "the stray worker must never be handed a task")
			},
		},
		{
			name: "worker_with_other_dedup_func_is_refused",
			run: func(t *testing.T, s *scenarioRun, want, _ *bucketDump) {
				stray := s.startWorker("stray", workerOpts{journalID: s.conf.journalID, dedupFunc: "", dedupReplicaLabels: s.conf.dedupReplicaLabels})
				s.startWorker("w1")
				converged(t, s, want)
				testutil.Equals(t, 0.0, counterTotal(t, stray.reg, "thanos_compact_worker_tasks_total"), "a worker deduplicating differently must never be handed a task")
			},
		},
		{
			name: "oversized_tasks_parked_then_unparked",
			conf: func(c nodeConfig) nodeConfig { c.maxTaskSeries = 1; return c },
			run: func(t *testing.T, s *scenarioRun, want, input *bucketDump) {
				w1 := s.startWorker("w1")
				res := s.converge(60 * time.Second)
				testutil.Assert(t, !res.halted, "parking must not halt: %v", res.lastErr)
				testutil.Assert(t, len(s.tasksInState(StateOversized)) > 0, "every plan is over the limit, so tasks must be parked")
				testutil.Equals(t, 0.0, counterTotal(t, w1.reg, "thanos_compact_worker_tasks_total"), "nothing may be dispatched")
				assertSameContent(t, input, dumpBucket(t, s.shared), "while parked")
				assertNoDeletionMarks(t, s.shared)

				// The operator raises the limit and unparks.
				c := s.conf
				c.maxTaskSeries = 0
				s.replaceManager(c)
				testutil.Assert(t, unparkAll(t, s) > 0, "nothing to unpark")
				s.waitFor("the parked entries to be dropped", func() bool { return len(s.tasksInState(StateOversized)) == 0 })
				converged(t, s, want)
			},
		},
		{
			name: "abandoned_task_parks_its_sources",
			// One plan at a time, so three crashes hit the same task.
			tenants: []tenantSpec{{name: "solo", series: 2, windows: 5, samples: 20}},
			conf:    func(c nodeConfig) nodeConfig { c.maxInflight = 1; return c },
			run: func(t *testing.T, s *scenarioRun, want, input *bucketDump) {
				s.inject(func() {
					for i := range s.conf.maxAttempts {
						w := s.startWorker(fmt.Sprintf("crash%d", i))
						_ = gateChunks(w.testWorker)
						s.waitLeasedBy(w.id)
						w.crash()
						<-w.done
					}
				})
				res := s.converge(60 * time.Second)
				testutil.Assert(t, !res.halted, "abandonment must not halt: %v", res.lastErr)
				abandoned := s.tasksInState(StateAbandoned)
				testutil.Equals(t, 1, len(abandoned), "the task must be abandoned after its attempts ran out")
				testutil.Assert(t, abandoned[0].LastError != nil && abandoned[0].LastError.Outcome == OutcomeAbandoned, "got %+v", abandoned[0].LastError)
				assertSameContent(t, input, dumpBucket(t, s.shared), "while parked")
				assertNoDeletionMarks(t, s.shared)

				testutil.Equals(t, 1, unparkAll(t, s))
				s.waitFor("the abandoned entry to be dropped", func() bool { return len(s.tasksInState(StateAbandoned)) == 0 })
				s.startWorker("healthy")
				converged(t, s, want)
			},
		},
		{
			name: "corrupted_source_fails_without_damage",
			run: func(t *testing.T, s *scenarioRun, _, input *bucketDump) {
				// Damage one of the plain tenant's blocks: it cannot be compacted.
				var damaged corpusBlock
				for _, b := range s.corpus.blocks {
					if b.tenant == "plain" {
						damaged = b
						break
					}
				}
				testutil.Ok(t, s.shared.Upload(context.Background(), filepath.Join(damaged.id.String(), block.IndexFilename), strings.NewReader("this is not an index")))
				s.startWorker("w1")
				s.startWorker("w2")

				// The plain group fails on every pass, as it would in the
				// binary; the run is judged once its task has burned its
				// attempts.
				s.passUntil("the corrupted task to fail", func() bool { return len(s.tasksInState(StateFailed)) > 0 })
				failed := s.tasksInState(StateFailed)[0]
				testutil.Equals(t, s.conf.maxAttempts, failed.Attempts)
				testutil.Assert(t, failed.LastError != nil && failed.LastError.Outcome == OutcomeFailedRetryable, "got %+v", failed.LastError)

				// The damaged group is exactly as uploaded: every source still
				// there and unmarked, and nothing produced for it.
				corpusIDs := map[ulid.ULID]struct{}{}
				for _, b := range s.corpus.blocks {
					corpusIDs[b.id] = struct{}{}
					if b.tenant != "plain" {
						continue
					}
					testutil.Equals(t, true, exists(t, s.shared, filepath.Join(b.id.String(), block.MetaFilename)), "source %s of the damaged group is gone", b.id)
					testutil.Equals(t, false, exists(t, s.shared, filepath.Join(b.id.String(), metadata.DeletionMarkFilename)), "source %s of the damaged group was marked", b.id)
				}
				for _, id := range blockIDs(t, s.shared) {
					if _, ok := corpusIDs[id]; ok {
						continue
					}
					m, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), s.shared, id)
					testutil.Ok(t, err)
					testutil.Assert(t, m.Thanos.Labels["tenant"] != "plain", "block %s was produced for the damaged group", id)
				}
			},
		},
		{
			name: "rollback_after_convergence",
			run: func(t *testing.T, s *scenarioRun, want, input *bucketDump) {
				s.startWorker("w1")
				s.startWorker("w2")
				converged(t, s, want)
				s.stopWorkers()
				s.currentManager().stop()

				plan, err := PlanRollback(context.Background(), log.NewNopLogger(), objstore.WithNoopInstr(s.shared), RollbackOptions{JournalID: s.conf.journalID})
				testutil.Ok(t, err)
				testutil.Assert(t, len(plan.Produced) > 0, "nothing to roll back")
				testutil.Ok(t, plan.Apply(context.Background(), log.NewNopLogger(), s.shared))

				// Exactly the input: the same block IDs, no marks, the same content.
				testutil.Equals(t, s.corpus.ids(), blockIDs(t, s.shared))
				assertNoDeletionMarks(t, s.shared)
				assertSameContent(t, input, dumpBucket(t, s.shared), "after the rollback")

				// The exit path of the trial: standalone takes over the bucket.
				c := s.conf
				c.mode = modeStandalone
				s.replaceManager(c)
				res := s.converge(90 * time.Second)
				testutil.Assert(t, !res.halted, "standalone halted after the rollback: %v", res.lastErr)
				assertSameContent(t, want, dumpBucket(t, s.shared), "standalone after the rollback")
			},
		},
		{
			name: "rollback_mid_run",
			run: func(t *testing.T, s *scenarioRun, _, input *bucketDump) {
				s.startWorker("w1")
				s.startWorker("w2")
				done := make(chan struct{})
				s.inject(func() {
					defer close(done)
					_ = s.converge(90 * time.Second)
				})
				s.waitFor("some work to have finished", func() bool { return len(s.tasksInState(StateCompleted)) >= 2 })
				// Everything stops at once, mid flight.
				s.stopWorkers()
				s.currentManager().stop()
				<-done

				plan, err := PlanRollback(context.Background(), log.NewNopLogger(), objstore.WithNoopInstr(s.shared), RollbackOptions{
					JournalID:             s.conf.journalID,
					AllowUnreadableBlocks: true, // Uploads cut off mid way leave partial blocks.
				})
				testutil.Ok(t, err)
				testutil.Ok(t, plan.Apply(context.Background(), log.NewNopLogger(), s.shared))

				got := dumpBucket(t, s.shared)
				testutil.Equals(t, s.corpus.ids(), got.servedIDs())
				assertNoDeletionMarks(t, s.shared)
				assertSameContent(t, input, got, "after the mid-run rollback")
			},
		},
	}
}

// TestScenarios runs the fault scenario suite. See the top of scenario_test.go.
func TestScenarios(t *testing.T) {
	skipUnlessScenarios(t)

	defaultCorpus := buildCorpus(t, "ha", haCorpus())
	goldens := map[string]*bucketDump{}
	inputs := map[string]*bucketDump{}
	corpora := map[string]*corpus{defaultCorpus.name: defaultCorpus}

	for _, sc := range scenarios() {
		t.Run(sc.name, func(t *testing.T) {
			conf := haNodeConfig()
			if sc.conf != nil {
				conf = sc.conf(conf)
			}
			conf = conf.withDefaults()

			c := defaultCorpus
			if sc.tenants != nil {
				c = buildCorpus(t, sc.name, sc.tenants)
				corpora[c.name] = c
			}
			key := fmt.Sprintf("%s/%v/%s", c.name, conf.dedupReplicaLabels, conf.dedupFunc)
			if goldens[key] == nil {
				goldens[key] = golden(t, c, conf)
				inputs[key] = inputDump(t, c)
				if c == defaultCorpus {
					// The corpus spans 48h, so the oracle must cover the
					// downsampled level too, or half the machinery is untested.
					var downsampled int
					for _, b := range goldens[key].blocks {
						if b.res > 0 {
							downsampled++
						}
					}
					testutil.Assert(t, downsampled > 0, "the standalone run left no downsampled block; the corpus is too short")
				}
			}

			s := newScenarioRun(t, c, conf)
			s.corpus = c
			t.Cleanup(s.wait)
			sc.run(t, s, goldens[key], inputs[key])
			s.wait()
		})
	}
}

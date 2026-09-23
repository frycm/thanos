# Trialing the distributed compactor

The distributed compactor (`--compact.mode=manager` / `--compact.mode=worker`, experimental) lets one block stream be compacted by several workers at once. This page describes how to try it against real data without putting that data at risk, in two stages that build on each other:

* **Stage A, the shadow run** proves the new mode is *correct* and that it actually *drains the backlog*, with zero blast radius: it runs against a copy of the raw blocks in a separate bucket.
* **Stage B, the cutover** switches a production shard over, with a rollback that returns the bucket to exactly the state the standalone compactor left behind.

Stage A is where you establish that B will work, and where you rehearse B's rollback before you need it. Do not skip it: the markers that make B reversible let you *undo* a bad outcome, but only A lets you *prove* a good one.

## Before Stage A: the fault scenario suite

The [failure coverage matrix](distributed-compaction-tests.md) describes the guarantees checked, the represented HA topology, CI coverage and remaining limits.

Before a bucket is involved at all, the failure modes the trial has to survive can be rehearsed in process, without a deployed bucket. `pkg/compact/distributed` carries a scenario suite that runs the whole control loop of the binary - sync, grouping, planning, dispatch, verification, garbage collection and both downsampling passes - against a synthetic bucket, with real workers and faults injected between the pieces. It is off by default because it is slow:

```bash
go test -tags slicelabels ./pkg/compact/distributed/ -run 'TestScenarios|TestGenericScenarios' -race -args -distributed.scenarios
```

The corpus is the deployment this mode exists for: an HA Prometheus pair (`prometheus_replica` as a series label inside the blocks) ingested by receive with replication factor 2 (`receiver_replica` as an external label), with two copies of most 2h windows and deliberately missing receiver copies in others, plus OTel collector replicas, ruler replicas, native histograms, missing receiver-copy windows and a small plain tenant. Penalty deduplication is configured with all four replica labels. The corpus spans 48h, so 5m downsampling is exercised as well.

The oracle is content, not blocks. The manager keeps several plans for one group in flight, so its intermediate block layout legitimately differs from the standalone compactor's; what must not differ is what a store gateway would serve. Every scenario ends by comparing every series and sample at every resolution, per external label set, against what the standalone compactor produced from the same blocks - the same comparison Stage A makes with `promtool`, without the bucket. On top of that every scenario checks that no served blocks overlap, that no task is left live in the journal, and that every worker-produced block still names sources a rollback could restore.

The scenarios of `TestScenarios`: the golden path; a single plan in flight per group, where the block layout has to match standalone's exactly; a worker killed mid task and its task reassigned; a manager restart that voids the old leases; two managers on one shard, where the first has to halt; a worker that cannot reach the journal and has to discard its finished work; an object store outage seen by every participant at once; a worker configured for another journal and one deduplicating with another function, both refused; oversized tasks parked and then released by an unpark request; a task whose workers keep crashing, abandoned with its source blocks parked and then released; a corrupted source block that fails without a deletion mark or a result; a rollback after convergence that returns the input exactly, followed by the standalone compactor taking the bucket over; and a rollback of a run cut off mid flight. `TestGenericScenarios` runs the scenarios the standalone compactor has to survive as well - an object store outage, a process crash and restart, a corrupted source, and failed publications of every block file - against a manager with two workers.

## Stage A: the shadow run

### Sync the raw blocks of one shard into a trial bucket

`thanos tools bucket replicate` does this continuously, scoped to the tenant (or shard) you want to trial, and only its raw, uncompacted blocks:

```bash
thanos tools bucket replicate \
  --objstore.config-file=production-bucket.yaml \
  --objstore-to.config-file=trial-bucket.yaml \
  --matcher='tenant="<the hot tenant>"' \
  --compaction=1 \
  --resolution=0s
```

The matcher applies to external labels, so a tenant's receiver replicas come along with it - which matters for deduplication, see below. Without `--single-run` it keeps replicating, so the trial bucket receives every new 2h block as production does.

### Run the distributed compactor against the trial bucket

Use the **same flags as the production compactor of that shard** - the same `--selector.relabel-config`, the same `--deduplication.replica-label` and `--deduplication.func`, the same retention - plus the mode:

```bash
# One manager per shard. It plans, dispatches, verifies and owns the bucket.
thanos compact --compact.mode=manager \
  --compact.manager.journal-id=<shard name> \
  --compact.manager.max-inflight-per-group=4 \
  --objstore.config-file=trial-bucket.yaml --wait ...

# As many workers as you like. They execute one task at a time and own nothing.
thanos compact --compact.mode=worker \
  --compact.manager.journal-id=<shard name> \
  --compact.worker.manager-address=dnssrv+_http._tcp.thanos-compact-manager.<namespace>.svc \
  --objstore.config-file=trial-bucket.yaml ...
```

Workers take the manager's `--deduplication.replica-label` and `--deduplication.func` too; the manager refuses a worker whose settings differ (see refused workers below). A worker works in `<data-dir>/compact-worker/<worker ID>`, so several workers can share one `--data-dir` as long as each has its own `--compact.worker.id`, which defaults to the hostname.

A store gateway and a querier on the trial bucket are enough for spot checks; the query frontend adds nothing to correctness.

The manager refills a group's worker slots as individual plans finish. Parked plans and conflicting time ranges do not stop the search for independent work. Completed and deferred ranges remain reserved until the next metadata sync; only then can their replacements participate in further compaction. The limit controls concurrent plans, and does not split a single large plan across workers.

### What to verify

**1. The output is identical to production's.** The shadow runs on the same inputs with the same compaction code, so for any time window its compacted block must hold *exactly* the same series and samples as the block production's standalone compactor produced for that window; only the ULIDs differ. `promtool` can show that for raw and compacted blocks, with two things to get right:

* `promtool tsdb dump` opens a TSDB *directory* and enumerates the ULID-named block directories inside it. Pointed at a block directory itself it finds no blocks, prints nothing, and a `diff` of two empty outputs succeeds - so download each block into its own parent directory, and **assert the dumps are non-empty** before comparing them.
* It reads series labels from the index, not the external labels in `meta.json`, so it says nothing about those - see step 3.

```bash
#!/usr/bin/env bash
# Fail on the first error, and on a failure anywhere in a pipeline: without
# pipefail a failed promtool is masked by the sort that follows it, and two
# failed dumps compare equal.
set -euo pipefail

# Each block in its own parent directory: promtool wants a TSDB root, not a block.
mkdir -p cmp/prod cmp/trial
<download production block> cmp/prod/<ulid>
<download trial block>      cmp/trial/<ulid>

promtool tsdb dump --min-time=<t0> --max-time=<t1> --match='{job="app"}' cmp/prod  | sort > prod.txt
promtool tsdb dump --min-time=<t0> --max-time=<t1> --match='{job="app"}' cmp/trial | sort > trial.txt

# An empty dump means promtool found no blocks - the comparison proves nothing.
for f in prod.txt trial.txt; do
  if [ ! -s "$f" ]; then echo "$f is empty: wrong directory layout?" >&2; exit 1; fi
done
diff prod.txt trial.txt
```

Sample windows and selectors rather than dumping everything; for a large tenant the full dumps are not practical, and a handful of windows across the 2h, 8h and daily compaction levels is what you are after.

**Downsampled blocks cannot be compared this way.** Thanos 5m and 1h blocks hold aggregate chunks that Prometheus's chunk reader does not decode. Compare them through a querier instead, which reads them natively: run a querier on each bucket (the production one and the trial one, each with its own store gateway) and query both for the same range at the downsampled resolution, then compare the results:

```bash
#!/usr/bin/env bash
set -euo pipefail

for q in prod trial; do
  # --fail-with-body makes an HTTP error fail the script instead of producing
  # an error document that would compare equal to the other side's; jq -e
  # then requires a successful, non-empty answer. The result series are sorted
  # by their labels so that ordering cannot cause a spurious difference.
  curl -sS --fail-with-body "http://thanos-query-$q/api/v1/query_range" \
    --data-urlencode 'query=sum by (job) (rate(http_requests_total[5m]))' \
    --data-urlencode "start=<t0>" --data-urlencode "end=<t1>" --data-urlencode 'step=5m' \
    --data-urlencode 'max_source_resolution=5m' \
    | jq -eS 'if .status == "success" and (.data.result | length) > 0
              then .data.result |= sort_by(.metric | tostring)
              else error("query failed or returned no series") end' \
    > "$q.json"
done
diff prod.json trial.json
```

Use `max_source_resolution=1h` for the 1h blocks. As a cheaper first pass, `thanos tools bucket inspect` on both buckets lists the series, sample and chunk counts of every block, which have to match block for block.

**2. The backlog drains.** This is the whole point. Watch, on the manager:

* `thanos_compact_manager_oldest_pending_task_seconds` and `thanos_compact_manager_pending_tasks` - both have to trend to zero and stay there at the 2h cadence. If they grow, the shard needs more workers or a higher `--compact.manager.max-inflight-per-group`.
* `thanos_compact_manager_abandoned_tasks_total` - must stay 0. An abandoned task means a block set repeatedly killed its worker without a report, or kept being aborted; its source blocks are parked, and the two gauges above sit innocently at zero while the shard makes no progress on them. Every abandonment needs an investigation (`state: "abandoned"` in the journal names the blocks).
* `thanos_compact_todo_compactions` - has to trend towards zero at the 2h cadence. This is the planner's own view of outstanding work, so it catches anything the task-level metrics miss.
* `thanos_compact_manager_tasks_total` by `outcome` - `failed_*` and `abandoned` outcomes should be rare and explained. It counts tasks that finished; an aborted task is requeued rather than finished, so aborts show on the workers, in `thanos_compact_worker_tasks_total` by `outcome`, where `aborted_*` outcomes should be rare and explained as well - except `aborted_worker_shutdown`, which every worker restart in the middle of a task produces.
* `thanos_compact_halted` - must stay 0.

**3. Deduplication works as it does in production, and the external labels are right.** If the shard uses `--deduplication.replica-label`, the `promtool` comparison in step 1 proves the series and samples match production's deduplicated blocks - but not the external labels, which live in `meta.json` and which `promtool` never reads, so it cannot show that `receiver_replica` was removed. Compare the block metadata separately:

```bash
thanos tools bucket inspect --objstore.config-file=production-bucket.yaml --selector='tenant="<the hot tenant>"' --sort-by=FROM
thanos tools bucket inspect --objstore.config-file=trial-bucket.yaml      --selector='tenant="<the hot tenant>"' --sort-by=FROM
```

The `LABELS` column of the trial's compacted blocks has to equal production's, replica label removed. The querier comparison in step 1 covers this as well, since a querier applies external labels to what it returns.

**4. Rehearse the rollback.** Run the stage B rollback against the trial bucket *before* the cutover, so that when you need it in production you have already seen it work - and rehearse it the way it has to be done in production, writers stopped first:

1. The trial manager must have been started with an extended `--delete-delay` (see stage B); a source that was physically deleted cannot be restored.
2. **Stop the manager and every worker**, and wait for in-flight tasks to end. A manager still running can upload another result after the plan is built, or re-mark a source the rollback just restored. The tool refuses to apply while the journal was written within `--manager-liveness-window` (a running manager writes it at least once per lease TTL, even when idle), so a refusal here means something is still running.
3. Plan, inspect, apply:

```bash
thanos tools bucket rollback-distributed-compaction --objstore.config-file=trial-bucket.yaml --journal-id=<shard name>
# inspect the plan, then:
thanos tools bucket rollback-distributed-compaction --objstore.config-file=trial-bucket.yaml --journal-id=<shard name> --no-dry-run
```

`--journal-id` is required: the tool undoes one manager's work. `--all-journals` undoes every manager's work in the bucket and has to be asked for by name, because in a bucket shared by several shards that is every shard's trial at once. If the tool refuses because some blocks' metadata cannot be read, find out what they are first; `--allow-unreadable-blocks` is only for blocks known to be unrelated, such as the remains of aborted uploads, since a marked source among them would not be restored.

Afterwards the trial bucket holds only the raw blocks that were replicated into it, and a standalone compactor pointed at it would pick up exactly where it would have without the trial.

### Parked tasks and refused workers

Two guardrails show up in the journal rather than in worker deaths:

* **Parked tasks.** A task that burned its whole attempt budget without a single report, or reached three times that budget in aborted executions (a worker shutting down does not count), is recorded as `abandoned`; a plan whose source blocks exceed `--compact.manager.max-task-series` (per output, when the plan names several) or `--compact.manager.max-task-index-size` (for the whole input, which the worker downloads) is recorded as `oversized` and never dispatched. Either way, every plan containing one of those source blocks is withheld from planning - the journal entry says why - until you release it by writing one empty object at `compact-manager/<journal id>/unpark/<task id>`, or the entry ages out of `--compact.manager.journal-retention`. Watch `thanos_compact_manager_parked_tasks`.
* **Refused workers.** A worker states its journal ID and its deduplication configuration when it asks for work, and the manager refuses a mismatch with an error naming the flag to fix - a worker on another shard's journal or merging with another function would otherwise fail invisibly or, worse, produce blocks that carry no trace of the difference.

### Reading the journal

The journal at `compact-manager/<journal id>/journal.json` is plain JSON with one entry per task under `tasks`. Besides `state`, `attempts`, `aborts` and `last_error`, three things in it follow from how the manager treats what workers upload:

* **`verified`.** A worker's block is published - part of the manager's view, and able to supersede the blocks it was made from - only once the manager has checked it against the plan and recorded that in the journal: `"state": "completed"` with `"verified": true`. Until then the manager's deduplication filter withholds it and counts it under `state="unpublished"` in `thanos_blocks_meta_synced`. The sources are marked for deletion only after the verdict is in the journal; if that write fails, the verdict is void, the outputs are rejected and the plan is redone. A completed entry that lists `outputs` without `verified` is either being verified at that moment or has its outputs rejected by the next maintenance tick.
* **`rejected_outputs`.** Blocks uploaded for the task that the manager will not accept: a result that failed verification, the outputs of a completed task that nobody verified because the manager stopped in between, or a block stamped with a finished task that the task does not account for - an attempt whose report never arrived, or an upload that finished after a takeover. Listed blocks supersede nothing in the manager's view, and maintenance deletes each of them, but only when the block's own `meta.json` says it was made for that task; any other ID is dropped from the list and the block left alone. An entry outlives `--compact.manager.journal-retention` while it lists rejected blocks, and so does a completed entry whose outputs were never verified. A list that does not empty means the manager cannot delete the blocks; its log says why.
* **Failed tombstones after a takeover.** A starting manager ends every task its predecessor left pending or leased: `"state": "failed"`, no lease, no outputs, and a `last_error` with outcome `aborted_ownership_lost` and the message `unfinished when generation <N> took over the journal`. The new manager replans that work under new task IDs. The tombstone keeps a block that a worker of the old generation still manages to upload from counting as published; such a block ends up in `rejected_outputs` and is deleted. Tombstones age out with the retention like any other finished task, and are expected after a manager restart that interrupted work.

## Stage B: the cutover

### What makes it reversible

Two things the distributed compactor records as it works, in normal operation and not only during trials:

* **Every block a worker produces carries its provenance** in the block's `meta.json` extensions, under `thanos_compact_distributed`: the output block ID, its immediate source block IDs, the task, the worker, the journal and the journal generation that produced it. This applies to compacted and downsampled blocks alike. It is in the extensions rather than in `Thanos.Source` on purpose: the consistency delay exempts compactor-sourced blocks, and a new source value would hide worker output from the manager's next sync while its sources were already marked for deletion.
* **Every deletion mark the manager writes names the manager and the task** in its details: `source of block compacted by distributed compactor; journal <id>; task <id>`. Rollback also removes `outdated block` marks written by garbage collection on the recorded sources after an interrupted manager finalization. Retention and operator marks are left alone.

Together they make `thanos tools bucket rollback-distributed-compaction` precise: it deletes exactly the blocks workers produced and restores exactly the blocks the manager replaced, and nothing else.

### Before the cutover

1. **Extend `--delete-delay`** on the shard's compactor to cover the whole trial window with margin. It is the physical safety window: a block marked for deletion is only actually deleted once the delay has passed, and a block that was physically deleted cannot be restored. The store gateway's `--ignore-deletion-marks-delay` only affects when it stops *serving* a marked block; removing the mark brings the block back at the next sync as long as the files still exist.
2. **Decide the go/no-go criteria** from stage A: the backlog metric threshold, and the windows whose `promtool` comparison must come out clean.

### Cutover

1. **Stop the standalone compactor of the shard.** Nothing protects against a standalone compactor and a manager running on the same shard at once; the journal detects a second *manager*, not a standalone compactor, and the first symptom would be an overlap halt. Never run both.
2. Start the manager with the shard's production flags plus `--compact.mode=manager --compact.manager.journal-id=<shard name>`, then the workers.
3. Watch the same metrics as in stage A. A halt (`thanos_compact_halted=1`) is the expected failure mode, not a disaster: it wedges the shard with the bucket intact, and you roll back at leisure. A halted manager also freezes its fleet: it revokes every lease, so workers discard what they were doing, fails the queued tasks in the journal with the halt as their reason, and hands out nothing until it is restarted. The fail-closed worker design means the bad outcomes are "the backlog grows", not "data is lost".

### Rolling back

Stop the manager and every worker, wait for in-flight tasks to end, then:

```bash
thanos tools bucket rollback-distributed-compaction --objstore.config-file=production-bucket.yaml --journal-id=<shard name>
# inspect the plan, then:
thanos tools bucket rollback-distributed-compaction --objstore.config-file=production-bucket.yaml --journal-id=<shard name> --no-dry-run
```

The tool first verifies that every original source is still present and can be restored, following the immediate-source provenance through intermediate worker outputs. It refuses the rollback if a required source is missing or has a foreign deletion mark. It then removes source deletion marks before deleting outputs, with descendants deleted before their parents. This order keeps data available if rollback is interrupted. Keep every writer stopped until rollback completes; otherwise garbage collection could mark the restored sources again. Finally, remove the journal at `compact-manager/<shard name>/journal.json` and restart the standalone compactor.

If rollback is interrupted, rerun the dry run and inspect it before applying again. An interrupted block deletion can leave files without `meta.json`; `--allow-unreadable-blocks` can be used once those remnants are confirmed to be unrelated to the remaining source graph. It does not override the requirement that every source needed for restoration exists.

The tool refuses to apply while the journal was written within `--manager-liveness-window` (15m by default; a running manager writes its journal at least once per `--compact.manager.lease-ttl`, even when idle), and it refuses to plan at all while any block's metadata cannot be read, since such a block might be a marked source it could not restore. Both refusals have explicit overrides, `--force` and `--allow-unreadable-blocks`; use them only once you have confirmed by other means that nothing is running and that the unreadable blocks are unrelated.

### Scheduler and worker failure handling

Journal reads and writes run outside the scheduler state lock, so an object-store stall does not block heartbeats. Snapshots are serialized before publication; callers recheck task ownership before rolling back a failed write. Maintenance deletes rejected blocks outside the lock as well. Once the manager has stopped, it writes nothing more to the journal - not even from maintenance, lease expiry, an oversized refusal or a halt, which have no context of their own - since its successor may already own it.

Aborted tasks retain their execution-attempt budget but have a separate cap of three times `--compact.manager.max-attempts`. Each abort delays requeueing by 30 seconds times the abort count, capped at the lease TTL. Reaching the cap parks the sources; new blocks in the same plan do not reset that protection. A worker shutting down in the middle of a task (`aborted_worker_shutdown`) is not charged at all: the task goes straight back to the queue, without a backoff and without counting against either budget, so a rolling restart never parks a task that takes longer than the restart cadence.

Workers heartbeat immediately and at most every third of the lease TTL. A worker cancels its task if no heartbeat is acknowledged within the TTL. Errors of the worker's own machine - a full or read-only disk, an I/O error, exhausted file descriptors or memory - are retryable worker failures rather than shard-wide halts; a missing or truncated file from the bucket keeps the halt the compactor raised. Correct the resource problem before releasing parked work.

Each worker works in `<data-dir>/compact-worker/<worker ID>`, so what has to be unique is `--compact.worker.id` (the hostname by default), not the data directory: workers and a manager can share one `--data-dir`. Startup removes abandoned ULID task directories left by crashes in the worker's directory; other directories and symlinks are preserved, and the worker removes its directory when it exits. Task IDs received from the manager must be canonical ULIDs before filesystem use.

Result metadata reads are attempted up to three times, with a five-second timeout per read and short delays. Verification still precedes all source retirement, and the remote executor honors the same deletion veto as the local executor.

# Trialing the distributed compactor

The distributed compactor (`--compact.mode=manager` / `--compact.mode=worker`, experimental) lets one block stream be compacted by several workers at once. This page describes how to try it against real data without putting that data at risk, in two stages that build on each other:

* **Stage A, the shadow run** proves the new mode is *correct* and that it actually *drains the backlog*, with zero blast radius: it runs against a copy of the raw blocks in a separate bucket.
* **Stage B, the cutover** switches a production shard over, with a rollback that returns the bucket to exactly the state the standalone compactor left behind.

Stage A is where you establish that B will work, and where you rehearse B's rollback before you need it. Do not skip it: the markers that make B reversible let you *undo* a bad outcome, but only A lets you *prove* a good one.

## Testing downsampling separately

The compactor defaults to `--compact.mode=standalone`. Stuck-block downsampling
is a separate opt-in, `--downsampling.enable-stuck-blocks`, which defaults to
`false`. A build containing both changes can therefore be deployed with the
existing local compaction and downsampling policy. Store gateway resolution
filtering can be deployed and tested independently of these compactor settings.

To test the changes in stages:

1. Keep the production compactor in standalone mode with stuck-block
   downsampling off while validating the new build and store gateway behavior.
2. Test stuck-block downsampling in standalone mode against a trial bucket,
   then enable it on the production standalone compactor after validation.
3. Run the distributed trial below with the same downsampling flag value as
   the standalone reference. Set the flag on the manager; workers need no
   separate setting for this policy.

Keep each compactor trial in its own bucket. Turning stuck-block downsampling
off stops new exceptions but does not remove the blocks it already produced.

## Before Stage A: the fault scenario suite

Before a bucket is involved at all, the failure modes the trial has to survive can be rehearsed in process, in a minute, on a laptop. `pkg/compact/distributed` carries a scenario suite that runs the whole control loop of the binary - sync, grouping, planning, dispatch, verification, garbage collection and both downsampling passes - against a synthetic bucket, with real workers and faults injected between the pieces. It is off by default because it is slow:

```bash
go test ./pkg/compact/distributed/ -run TestScenarios -race -args -distributed.scenarios
```

The corpus is the deployment this mode exists for: an HA Prometheus pair (`prometheus_replica` as a series label inside the blocks) ingested by receive with replication factor 2 (`receiver_replica` as an external label), so every 2h window is two identical blocks holding both Prometheus replicas' series, plus a small plain tenant so that two groups are always in play. The corpus spans 48h, so 5m downsampling is exercised as well.

The oracle is content, not blocks. The manager keeps several plans for one group in flight, so its intermediate block layout legitimately differs from the standalone compactor's; what must not differ is what a store gateway would serve. Every scenario ends by comparing every series and sample at every resolution, per external label set, against what the standalone compactor produced from the same blocks - the same comparison Stage A makes with `promtool`, without the bucket. On top of that every scenario checks that no served blocks overlap, that no task is left live in the journal, and that every worker-produced block still names sources a rollback could restore.

The scenarios: the golden path; a single plan in flight per group, where the block layout has to match standalone's exactly; a worker killed mid task and its task reassigned; a manager restart that voids the old leases; two managers on one shard, where the first has to halt; a worker that cannot reach the journal and has to discard its finished work; an object store outage seen by every participant at once; a corrupted source block that fails without a deletion mark or a result; a rollback after convergence that returns the input exactly, followed by the standalone compactor taking the bucket over; and a rollback of a run cut off mid flight.

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

A store gateway and a querier on the trial bucket are enough for spot checks; the query frontend adds nothing to correctness.

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
* `thanos_compact_manager_tasks_total` by `outcome` - `aborted_*` and `failed_*` outcomes should be rare and explained.
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

* **Parked tasks.** A task that burned its whole attempt budget without a single report is recorded as `abandoned`; a plan whose source blocks exceed `--compact.manager.max-task-series` or `--compact.manager.max-task-index-size` is recorded as `oversized` and never dispatched. Either way the exact source block set is withheld from planning - the journal entry says why - until you release it by writing one empty object at `compact-manager/<journal id>/unpark/<task id>`, or the entry ages out of `--compact.manager.journal-retention`. Watch `thanos_compact_manager_parked_tasks`.
* **Refused workers.** A worker states its journal ID and its deduplication configuration when it asks for work, and the manager refuses a mismatch with an error naming the flag to fix - a worker on another shard's journal or merging with another function would otherwise fail invisibly or, worse, produce blocks that carry no trace of the difference.

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
3. Watch the same metrics as in stage A. A halt (`thanos_compact_halted=1`) is the expected failure mode, not a disaster: it wedges the shard with the bucket intact, and you roll back at leisure. The fail-closed worker design means the bad outcomes are "the backlog grows", not "data is lost".

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

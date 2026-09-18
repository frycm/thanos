---
type: proposal
title: Compactor block splitting by series
status: proposed
owner: frycm
menu: proposals-accepted
---

* **Owners:**
  * @frycm

* **Related Tickets:**
  * [#1424 compactor: write postings: exceeding max size of 64GiB](https://github.com/thanos-io/thanos/issues/1424)
  * [#3068 Limit size of blocks to X bytes on compaction](https://github.com/thanos-io/thanos/issues/3068)
  * [#5437 Vertical Block Sharding](https://github.com/thanos-io/thanos/issues/5437)
  * [#3390](https://github.com/thanos-io/thanos/pull/3390) and [#4191](https://github.com/thanos-io/thanos/pull/4191), earlier proposal drafts
  * [Cortex: Timeseries Partitioning in Compactor](https://github.com/cortexproject/cortex/pull/4843)
  * [Mimir: split-and-merge compactor](https://grafana.com/docs/mimir/latest/references/architecture/components/compactor/)

* **Other docs:**
  * [Compactor](../components/compact.md)

> TL;DR: When a compaction plan would produce an index larger than the configured limit, the compactor writes M output blocks for the same time range instead of one, each holding the series whose label hash falls into shard `i` of `M`. Shard identity is an external label, so the existing grouper, planner, metadata deduplication and store gateway treat each shard as its own compaction stream. Nothing is frozen with a no-compact marker any more unless the plan would need more shards than allowed.

## Why

The Prometheus index format addresses postings with 32-bit offsets and 16-byte alignment, which caps an index at 64 GiB. Thanos cannot change that format. Today the compactor protects itself with `largeTotalIndexSizeFilter`: before executing a plan it sums the index sizes of the source blocks, and as soon as the running total reaches 85% of `--compact.block-max-index-size` it writes a `no-compact-mark.json` for the biggest source block and replans without it.

This has three consequences that matter for large tenants:

1. **Blocks are frozen forever.** A marked block never compacts or deduplicates again. Two HA replicas with 30 GiB indexes each sum to 60 GiB, so the larger one is marked even though the deduplicated result would be about 30 GiB. The marker survives a raised limit and must be removed by hand.
2. **Downsampling gets stuck.** A frozen 2-day block never reaches the span required for 5m and 1h downsampling. Older-range store gateways that are meant to serve downsampled data end up serving raw blocks or nothing.
3. **One block is one unit of work.** A single compaction, download, upload, downsample, index-header load or rollback is bounded by the largest block. Neither the compactor nor the store gateway can parallelize below block granularity.

```
Today: the planner freezes the biggest block before it can be deduplicated.

   replica A, 30 GiB index      replica B, 30 GiB index
   ┌──────────────────────┐     ┌──────────────────────┐
   │  block A  [t0, t1)   │     │  block B  [t0, t1)   │
   └──────────┬───────────┘     └──────────┬───────────┘
              │       plan = {A, B}        │
              └─────────────┬──────────────┘
                            ▼
              sum of indexes = 60 GiB  >  0.85 * 64 GiB
                            │
                            ▼
              no-compact-mark.json on B  (index-size-exceeding)
                            │
                            ▼
              A compacts alone, B stays raw and overlapping forever
```

Cortex and Mimir both solved this by splitting compaction output by series. Their approaches differ and neither is directly portable to Thanos; the differences are discussed under Alternatives.

## Pitfalls of the current solution

* The estimate assumes indexes share no bytes, so it over-estimates by up to the replication factor for overlapping blocks.
* Marking is permanent and invisible to most users until they inspect the bucket.
* Blocks that grow through normal horizontal compaction drift toward the limit and freeze as a steady state, so this is not an edge case for tenants above roughly 50 million series per stream.

## Goals

* Compaction never fails or freezes a block because of index size when the source blocks fit into the configured number of shards.
* Overlapping blocks can always be deduplicated, regardless of their size.
* Shard blocks behave like ordinary blocks for every existing component: grouper, planner, downsampler, retention, metadata deduplication, store gateway, querier, and bucket tools.
* The change is opt-in and defaults to today's behaviour.
* The distributed compactor (manager/worker) inherits splitting without protocol changes and can later parallelize it one shard per task.

## Non-goals

* Lifting the 64 GiB index limit itself.
* Changing the TSDB block or index format.
* Re-merging shards back into one block, or changing the shard count of already split data. A tool for that can follow.
* Query-time block skipping by shard. It is a natural follow-up and is described under Future work.
* Splitting at ingestion time in Receive or Sidecar.

## Audience

Operators running Thanos with streams above tens of millions of series, and contributors working on the compactor and store gateway.

## How

### Terminology

* **Plan**: the set of source blocks the planner selected for one compaction.
* **Shard**: one of M output blocks of a split plan. Shard `i` holds every series whose hash falls into bucket `i` of `M`.
* **Shard label**: the external label `__compactor_shard__` with value `i_of_M`, 1-based, for example `3_of_8`. A block without the label is unsplit.
* **Shard group**: the compaction group formed by blocks that share all external labels including the shard label.

### Shard identity is an external label

The shard ID is stored as an external label in `meta.json`, in `thanos.labels`, and not in `thanos.extensions`. This single choice is what keeps the rest of Thanos unchanged:

* `DefaultGrouper` keys groups by resolution and label hash, so every shard is its own group and is only ever compacted with blocks of the same shard.
* The planner's overlap detection only sees blocks of one group, so shards of the same time range are never mistaken for overlapping replicas.
* `DefaultDeduplicateFilter` in the metadata fetcher, used by both the compactor and the store gateway, drops any block whose compaction sources are contained in another block's sources. All M shards of one plan record the same sources, so the filter has to know one thing about shards: it deduplicates a stream's shards together with the blocks they were made from, and a shard supersedes its unsplit sources and the coarser shards of its own lineage but never a sibling of the same plan. Without that, either M-1 shards would vanish from view, or the retired sources would stay visible to the compactor until the deletion delay passed and be compacted again and again.
* The store gateway's `--selector.relabel-config` can select on the shard label, which spreads shards of one stream across store gateways. That is the acceptance criterion of #5437.

The cost is that the label is visible to the read path, which is handled in the store gateway (see below).

```
Groups before and after the first split of stream {cluster="eu1"}.

   before                              after
   ────────────────────────────        ──────────────────────────────────────────
   group 0@{cluster="eu1"}             group 0@{cluster="eu1"}                (unsplit blocks)
                                       group 0@{cluster="eu1", shard="1_of_4"}
                                       group 0@{cluster="eu1", shard="2_of_4"}
                                       group 0@{cluster="eu1", shard="3_of_4"}
                                       group 0@{cluster="eu1", shard="4_of_4"}

   (shard= abbreviates __compactor_shard__= throughout this document)
```

### Which series goes to which shard

```
shard(series) = labels.StableHash(series labels without --compact.block-split.ignore-labels) mod M
```

`labels.StableHash` from the Prometheus `labels` package is stable across Go versions and label implementations. The hash is taken over the series labels stored in the block, without external labels. Replica labels configured with `--deduplication.replica-label` are external labels, so the same series from two replicas hashes to the same shard and is deduplicated inside that shard.

Replica labels that live inside the series - HA Prometheus pairs behind a receiver, agents over remote write - would hash to different shards. `--compact.block-split.ignore-labels` names series labels to leave out of the hash (the partition records them as `without`), so both replicas of a series share a shard, where a later compaction that deduplicates by a series label could merge them. A series carrying none of the labels hashes exactly as it would without the flag. The compactor itself still relies on query-time deduplication for such replicas; the flag keeps the shards ready for the day it does not.

#### Staged shards are withheld, and sets outlive their members

A shard whose set is incomplete is unpublished: it supersedes nothing, and the sources stay. That alone is not enough. The compactor could still compact such a shard further - merge it with the next range of its group, say - and the result, a block with no set of its own and therefore published, would list the unsplit sources of the failed split among its sources and supersede them, with the series of the missing sibling gone. The compactor's deduplication filter therefore withholds unpublished blocks from its view altogether (`HideUnpublished`), after letting them count as duplicates of a later, published attempt at the same plan so that garbage collection retires the leftovers. Store gateways keep serving such blocks next to their sources, which is harmless.

Withholding needs sets that outlive their members. Once one shard of a split is compacted on - merged horizontally, re-split - and retired, its siblings' own sets can never be complete again, and they would be withheld although they are the only holders of their series. Every plan over shard blocks therefore names as **siblings** the blocks of the compactor's view that share a set with one of its sources - the sources' own sets, and the sets of other blocks that named a source as their sibling - and the executor records them in every output's set. A block is published by its own complete set or by any other complete set that names it. The rule is closed under further compaction: whatever retires a block re-certifies everything that block's sets certified. `TestShardTreeNeverLosesCoverage` drives the real filter through random histories of splits that fail after any number of shards, re-splits, horizontal merges and garbage collection, and checks after every step that every series of every original block is still held in the bucket and in the view; the span-based sibling rule tried first failed it within a few seeds.

The set of ignored labels is part of a stream's shard lineage and must not change once shards exist: a shard re-split under another hash holds series that none of its finer shards claims. The executor guards this - the populators of a plan's partitions tally the series they walked and kept, and a plan whose partitions together keep fewer series than they walked is refused with a halt error before anything is uploaded - so a changed flag stops the compactor rather than losing series. To change the labels, unsplit shards would have to be merged whole first, which is not supported.

### Deciding whether and how much to split

Splitting is decided per plan by the planner, from the source blocks' metadata, before anything is downloaded. It replaces the marking step of `largeTotalIndexSizeFilter` when splitting is enabled. The decision travels with the plan: since the [execution seam](https://github.com/frycm/thanos/pull/10) a plan names its outputs - for each block to produce, its external labels and, optionally, the partition of the series it holds - and the executor produces exactly what the plan says. The planner is the right place for the decision because it sees the whole bucket, which the rules below need; an executor, in this process or on a worker elsewhere, only sees the plan it was handed.

```
inputs      : index bytes of every source block (from thanos.files, or bucket attributes)
              series count of every source block (from stats)
              existing shard count S of the sources (0 for unsplit blocks)

k_index     = ceil( sum(index bytes) / (0.85 * block-max-index-size) )
k_series    = ceil( sum(series)      / block-split.max-series )         (0 = ignore)
k           = next power of two >= max(k_index, k_series, 1)            parts the sources need
M           = min( max( k * max(S, 1), F ), max )                        F = finest count present in the lineage: every shard of the stream for unsplit sources, the shards refining i_of_S for sharded ones

if k_index * max(S, 1) > max : refuse the plan: mark the biggest block, as today
if M == S                    : compact normally, output keeps the sources' shard label
if M >  S                    : split into M shards
```

Only the index size is a hard limit, and only it refuses a plan. The series target merely sizes the split: a plan that would want more shards than `max` for its series is split into `max` and compacted.

The estimate measures the sources. For sources that are already shard `i` of `S`, it measures that shard's data alone: if they need `k` parts, each part is one of `S × k` shards of the whole stream, so the count grows by the factor `k`, not to `k`. Two 40 GiB inputs of `1_of_4` need two parts and become `1_of_8` and `5_of_8`; taking `max(k, S) = 4` instead would leave the oversized shard as it is.

Points worth noting:

* The estimate is the same worst-case sum the filter uses today. It overestimates for overlapping blocks, so shards come out smaller than the limit. That is acceptable: the cost of one shard too many is one more block per range, the cost of one too few is a failed compaction.
* M is always a power of two. This is what makes later re-splitting cheap (next section).
* For a plan whose sources already carry a shard label, S is their count. All sources in one group share the same label, so S is well defined.
* The cap is judged per lineage, by the split planner itself. The scaled index-size filter (`max-shards × block-max-index-size`) guards the unsplit path, but a shard group's plan can be small in bytes and still have no room under the cap once its count is multiplied. Such a plan is refused the way the filter refuses one: the biggest source block is marked no-compact with the existing reason, `thanos_compact_block_split_fallbacks_total` counts it, and planning goes on without that block.

```
Split of one plan into M = 4 shards.

   sources (unsplit, same group, adjacent or overlapping in time)
   ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
   │  src1  │ │  src2  │ │  src3  │ │  src4  │    sum(index) = 100 GiB
   └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘    M = pow2(ceil(100 / 54.4)) = 2 → 2
       └──────────┴─────┬────┴──────────┘         (example continues with M = 4 for clarity)
                        ▼
              download once, verify once
                        │
        ┌───────────────┼───────────────┬───────────────┐
        ▼               ▼               ▼               ▼
   populate         populate        populate        populate
   hash%4 == 0      hash%4 == 1     hash%4 == 2     hash%4 == 3
        │               │               │               │
        ▼               ▼               ▼               ▼
   ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
   │ 1_of_4  │     │ 2_of_4  │     │ 3_of_4  │     │ 4_of_4  │   same [minT, maxT)
   │ sources:│     │ sources:│     │ sources:│     │ sources:│   same resolution
   │ 1,2,3,4 │     │ 1,2,3,4 │     │ 1,2,3,4 │     │ 1,2,3,4 │   labels + shard label
   └─────────┘     └─────────┘     └─────────┘     └─────────┘
                        │
                        ▼
              upload all M, then mark src1..src4 for deletion
```

### Executing a split

The split planner wraps the compactor's planner and implements the seam's `OutputPlanner`: once the inner planner has chosen the sources, it computes M as above and, if M differs from S, names one output per shard - the group's labels plus `__compactor_shard__ = "i_of_M"`, and the series partition `index i of count M`. If M equals S it names nothing, and the plan produces the one block it always did.

The in-process executor (the code path used by the standalone compactor and, in the distributed compactor, by every worker) knows nothing about shards. For a plan with outputs it:

1. Downloads and verifies the sources exactly as today.
2. Runs one compaction per output over the same downloaded directories. An output with a series partition uses the seam's `PartitionedBlockPopulator`, which filters each source block's postings to the series of the partition, collects only the symbols those series use, and hands the filtered sets to the standard merge and deduplication code. Everything downstream, including the vertical merge function and tombstone handling, is unchanged.
3. Sets each output's labels from the plan, so every shard block carries `__compactor_shard__ = "i_of_M"` and all other labels of the group.
4. Uploads all outputs. Only after every upload succeeded, marks the sources for deletion.

Reading the sources M times is the price of not forking the TSDB compactor; upstream Prometheus has no multi-output compaction. It is a CPU and disk-read cost only, not an object storage cost, since the sources are downloaded once. In the distributed compactor the M passes become M parallel tasks (see below), which removes the wall-clock cost.

An empty shard, one that receives no series, produces no block, exactly as an empty compaction does today. Source coverage is still complete because the remaining shards record every source.

### Life of a stream over time

```
Time range grid for one stream, compaction ranges 2h → 2d → 14d.
Each cell is one block. ░ = unsplit, digits = shard i of M.

level 1 (2h)   ░░░░░░░░░░░░░░░░░░░░░░░░  ░░░░░░░░░░░░░░░░░░░░░░░░
                       │                          │
level 2 (2d)   ░░░░░░░░░░░░░░░░░░░░░░░░  ░░░░░░░░░░░░░░░░░░░░░░░░    fits, no split
                       │                          │
                       └──────────┬───────────────┘
                                  ▼      estimate exceeds limit → M = 4
level 3 (14d)  1111111111111111111111111111111111111111111111111
               2222222222222222222222222222222222222222222222222      four blocks,
               3333333333333333333333333333333333333333333333333      same range,
               4444444444444444444444444444444444444444444444444      disjoint series
```

After the split the stream has five groups. The unsplit group keeps receiving fresh 2h blocks and compacting them to 2d. Whenever a 2d-to-14d plan in the unsplit group exceeds the limit it splits again, so the new shards join the existing shard groups. Inside each shard group, adjacent 14d blocks are compacted horizontally like any other blocks, and overlapping blocks are deduplicated vertically.

Three rules keep the unsplit group and the shard groups of one stream consistent:

* **Never fewer shards than the lineage has.** If the estimate for a later plan in the unsplit group - a fresh time range - yields M = 2 while shard groups with M = 4 exist, the outputs `1_of_2` and `2_of_2` would form two new groups that could never merge with the `_of_4` ones. The planner therefore looks up the finest shard count present for the stream's label set and resolution in the compactor's synced view and never chooses a smaller M. The same holds inside a shard group: once one range of `1_of_4` has been re-split into `1_of_8` and `5_of_8`, a plan of `1_of_4` over another range produces `_of_8` shards even if its own estimate would not need them, and a `1_of_4` block left alone - with nothing at count 4 to compact with any more - is planned on its own and split up to the lineage's count, covered by finer shards or not; otherwise the ranges at count 4 and at count 8 could never compact into one, and a shard stuck below the downsampling span would never be downsampled, which is what the scenario suite caught once shard groups re-split. This is metadata the compactor already holds; no extra state is stored.
* **"Newest" is judged per stream.** The planner leaves a group's newest time range alone so that a just uploaded block gets its siblings. A stream's fresh blocks arrive in its unsplit group, so a shard group's last range is never the stream's newest, yet the planner would treat it as such and never compact it: the last 48h of every shard would stay at 8h blocks, never downsampled. When splitting is enabled the planner therefore plans a group's newest block like any other whenever the stream has a newer block in another of its groups.
* **Stragglers are split on their own.** A block can be left alone in its range with nothing to compact against - an unsplit block, or a shard block whose group re-split that range while the block was still being made: the window one replica missed while its neighbours were merged vertically and split, a late upload, a backfill. The planner never compacts a lone block, so it would stay unsplit forever, never downsampled and never retired. When the planner has nothing for a group and a block's range is already covered by shards that refine the block's own shard - any shard for an unsplit block, the finer shards of its lineage for a shard block - that block is planned alone, with exactly the shards that cover the range as its outputs - whatever their counts, since a re-split leaves an uneven tree - so that every piece joins the group holding the rest of its series and compacts with it; a part of the hash space no present shard covers, because that shard was empty when the range was split, gets a new shard at the finest count present. Splitting such a block into the stream's largest count instead would strand slivers in groups with nothing to grow into, never downsampled. Coverage is the only guard: a covered block is not a fresh upload waiting for siblings, whatever its position in the unsplit group, and after the other stragglers are gone it is the group's newest block. The compactor's group loop, which otherwise skips groups holding a single block as having nothing to compact, hands such groups to a planner that declares it has work for them. A backfill into a covered range is therefore split block by block as it lands; the shard groups compact the pieces horizontally afterwards. This is what the first run of the scenario suite caught: without it the split run served the missing-replica windows as raw blocks next to the shards.

### Re-splitting a shard that grew too large

A shard group can itself outgrow the limit. Because shard counts are powers of two, a shard of `M` splits cleanly into two shards of `2M`, and the planner never needs to read across shard groups:

```
hash mod 8 == 3   ⟺   hash mod 16 == 3  or  hash mod 16 == 11

   group shard=3_of_8                 group shard=3_of_16        group shard=11_of_16
   ┌────────────┬────────────┐        ┌─────────────────────┐    ┌─────────────────────┐
   │ block a    │ block b    │  ───▶  │ series of a,b with  │    │ series of a,b with  │
   │ [t0,t1)    │ [t1,t2)    │        │ hash%16 == 3        │    │ hash%16 == 11       │
   └────────────┴────────────┘        └─────────────────────┘    └─────────────────────┘
```

The planner computes M from the plan as usual; with S = 8 and an estimate above 8 it gets M = 16 and names two outputs, `count: 16, index: 3` and `count: 16, index: 11`, which land in two new groups. This is the same modulo compatibility Cortex relies on, without its group-info files, and it gives the adaptivity that #4191 sought with sixteen hash functions.

The reverse direction, merging `3_of_16` and `11_of_16` back into `3_of_8`, is not part of this proposal.

### Planner changes

`largeTotalIndexSizeFilter` stays for the disabled case. When `--compact.block-split.max-shards` is greater than zero the filter is not installed; instead the planner decides per plan as described. The only marking that remains is the fallback when M would exceed the maximum, which keeps the current marker reason and message so existing alerts continue to work.

`verticalCompactionDownsampleFilter` is unaffected: shards of the same range live in different groups and never appear in one overlapping plan.

### Vertical compaction and deduplication

Overlapping sources are the case that motivates this proposal. They need no special handling: the shard populator runs on every source block of the plan, so shard `i` receives the same series from every replica, and the vertical merge function deduplicates them inside the shard.

```
   replica A [t0,t1)  30 GiB      replica B [t0,t1)  30 GiB
          │                              │
          └──────────┬───────────────────┘   estimate 60 GiB → M = 2
                     ▼
        ┌────────────┴────────────┐
        ▼                         ▼
   shard 1_of_2               shard 2_of_2
   A∩shard1 ⊎ B∩shard1        A∩shard2 ⊎ B∩shard2
   deduplicated, ~15 GiB      deduplicated, ~15 GiB
```

### Read path

Shard blocks are ordinary blocks with one extra external label. The store gateway loads them, and a query touching the range reads M blocks instead of one. Series are disjoint across shards, so the querier's deduplication is not involved and result sets are unchanged.

The label must not reach users. The store gateway already removes external labels per request for replica deduplication (`extLsetToRemove` in `pkg/store/bucket.go`); the shard label is added to that set unconditionally. `LabelNames` and `LabelValues` skip it the same way. The querier and the rest of the read path do not know the label exists.

```
                       query [t0, t1) {job="api"}
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │      store gateway    │
                     │  blocks: 1_of_4 ..    │
                     │          4_of_4       │
                     └──┬─────┬─────┬─────┬──┘
                        ▼     ▼     ▼     ▼
                      ┌───┐ ┌───┐ ┌───┐ ┌───┐     each block: matchers, postings,
                      │ 1 │ │ 2 │ │ 3 │ │ 4 │     chunks, as today
                      └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘
                        └─────┴──┬──┴─────┘
                                 ▼
                     strip __compactor_shard__ from every series
                                 │
                                 ▼
                              querier   (no change)
```

With `--selector.relabel-config` an operator can pin `shard=1_of_4` and `shard=2_of_4` to one store gateway and the other two to another, so one large stream is served by several store gateways in parallel and the index-header memory per gateway shrinks accordingly.

### Downsampling, retention and tools

* **Downsampling** operates on one block at a time and copies its labels. A raw shard downsamples to a 5m shard with the same label and then to a 1h shard. Downsampling of a split range is M independent jobs, which is a gain for the distributed compactor.
* **Retention** is per resolution and unaware of labels. No change.
* **`thanos tools bucket inspect`, `ls`, `verify`** show the label like any other external label. `verify` with the overlap check groups by labels and reports no overlap between shards.
* **`thanos tools bucket rewrite`** works on one block and preserves labels.

### The one invariant every consumer must respect

A shard block records the full source set of its plan while holding a fraction of the series. Any logic that treats one block as superseding another because its sources contain the other's sources is only correct between blocks with identical external labels. This holds for:

* the metadata `DefaultDeduplicateFilter`, which scopes by group key without the shard label, applies the lineage rule above, requires a shard's whole output set to be present before it supersedes anything, and orders blocks with the same sources by shard count, finest first, so that a shard re-split on its own is superseded by the finer shards made from it although they record the very same sources and are younger;
* the downsampling planner's coverage (`downsample.Plan`, in the seam), which judges "already downsampled" per set of external labels, not per source ULID: shards of one plan record the same sources, and downsampling one shard must not silence its siblings after a restart;
* the store resolution filter's coverage check ([PR #3](https://github.com/frycm/thanos/pull/3)), which scopes by labels;
* the stuck-block downsampling fences ([PR #2](https://github.com/frycm/thanos/pull/2)), which scope by label group.

Each of these gets a regression test with two shard blocks of one plan, so a future change that compares sources across label sets fails loudly.

### Failure handling

* **Crash or failed upload after some shards were published, before the sources were marked.** Source ancestry alone would be dangerous here: the first shard uploaded records every source of the plan, and on its own it would let the deduplication filter hide the sources and garbage collection retire them, with the series of the missing shards gone. The [execution seam](https://github.com/frycm/thanos/pull/10) therefore publishes the outputs of a plan as a set. Every output block records the whole set - its index among the plan's outputs, how many were planned, and the ULIDs of every block the compaction produced, all known before the first upload because the outputs are compacted first and uploaded after; a planned shard absent from the set held no series. The deduplication filter, in the compactor and in the store gateway, lets a shard supersede its sources only once every block of its set is present, and never lets a shard supersede another of its own set. So the next run sees the sources still in view, replans them and produces every shard again; the complete set supersedes the leftover shard of the interrupted attempt, ULID order notwithstanding, and garbage collection deletes it. The cost is repeated work, not duplicated or lost data. `split_survives_partial_publication` in the scenario suite fails the executor right after the first shard's `meta.json` is in the bucket and checks exactly this against the unsplit oracle.
* **Crash during an upload.** The partial block has no `meta.json` and is removed by the existing partial-upload cleanup after the usual delay.
* **The shard cap is lowered below a count the stream already has.** A plan that needs no further split always fits, whatever its lineage's count, so ordinary compaction within existing shards goes on; only a plan that would need more shards than the cap is refused.
* **A source records no index size.** Blocks from before Thanos recorded file sizes are measured in the bucket, as the index size filter measures them, so that the estimate never counts them as empty. A block whose index is missing from the bucket counts as unknown; the compaction itself reports what is wrong with it.
* **A backfill spans ranges with different shard layouts.** Covering shards are only found when one layout spans the whole block, so a block crossing a range boundary between two layouts is never split and stays raw, visible as a block that never compacts. Two-hour blocks never cross a two-day boundary; a hand-made larger block can. Splitting such a block at layout boundaries is future work.
* **A shard still exceeds the limit at execution time.** The compaction fails with the 64 GiB error and the group halts, as an oversized compaction does today. The estimate already overshoots, so this signals a badly skewed hash distribution; the operator raises `max-shards` or lowers `max-series`. Doubling M automatically on such a failure is future work: the planner has no feedback path from execution yet.
* **`max-shards` too low for a stream, or for a lineage.** The plan is refused at planning time, with the current marker reason, so the operator sees the same signal as today and can raise the limit.

### Distributed compactor (manager/worker)

The [manager/worker split](https://github.com/frycm/thanos/pull/1) plans on the manager and executes plans through the same in-process executor on the worker. Because the plan names its outputs, the manager's planner makes the split decision with its bucket-wide view, the task carries the outputs to the worker as data, the worker produces exactly those, and the manager verifies every result block against one of the plan's outputs. None of that is shard-specific, so the two features are independent of each other and merge in either order. Splitting therefore arrives in two steps:

1. **Inherited.** A worker that receives a plan with M outputs runs all M passes itself and reports M blocks. The manager verifies source coverage and time coverage over the union as it does today, and each block's labels against the plan's outputs. The manager judges a task's size per output rather than per plan, since the output is what the shard limit bounds; input size limits remain meaningful for worker disk.
2. **Fanned out.** The manager dispatches one task per output, with identical sources and one output each. Workers run one pass each. The manager waits for all M results, verifies the union, and only then marks the sources. Completed shards are recorded in the journal, so a failed shard is re-issued alone.

```
Fanned-out split in the distributed compactor.

   manager                                      workers
   ───────                                      ───────
   plan {src1..src4}, M = 4
     │
     ├── task A  sources=1..4  shard=1_of_4 ───▶ w1: download, populate shard 1, upload
     ├── task B  sources=1..4  shard=2_of_4 ───▶ w2: download, populate shard 2, upload
     ├── task C  sources=1..4  shard=3_of_4 ───▶ w3: download, populate shard 3, upload
     └── task D  sources=1..4  shard=4_of_4 ───▶ w4: download, populate shard 4, upload
     │
     ◀── results A, B, C, D  (block IDs + meta checksums)
     │
     verify: every source covered by the union, ranges identical,
             labels = group labels + shard label, provenance per block
     │
     mark src1..src4 for deletion; record plan complete in journal
```

Sources and time envelopes are reserved per plan in the manager's scheduling loop, so the M tasks of one plan never conflict with other plans of the group. The rollback tool follows per-block provenance and treats a plan's shard set as one unit.

### Configuration

| Flag                               | Default | Meaning                                                                                                                  |
|------------------------------------|---------|--------------------------------------------------------------------------------------------------------------------------|
| `--compact.block-split.max-shards` | `0`     | Maximum shards per plan. `0` disables splitting and keeps today's marking behaviour.                                     |
| `--compact.block-max-index-size`   | `64GB`  | Existing hidden flag. With splitting enabled it is the per-shard target; the estimate applies the existing 15% headroom. |
| `--compact.block-split.max-series` | `0`     | Optional per-shard series target. `0` ignores series counts.                                                             |

The store gateway needs no flag. Stripping the shard label is unconditional because the label is reserved.

### Observability

* `thanos_compact_block_splits_total`: plans that were split.
* `thanos_compact_block_split_shards`: histogram of the number of shards per split.
* `thanos_compact_block_split_fallbacks_total`: plans refused because their lineage would need more shards than `max-shards` allows; each such refusal also marks a block, so `thanos_compact_blocks_marked_total{reason="index-size-exceeding"}` moves with it.
* The existing `thanos_compact_blocks_marked_total{reason="index-size-exceeding"}` should trend to zero once splitting is enabled; that is the rollout signal.
* Compaction logs include `shard` and `shard_count` for every output.

### Rollout

1. Deploy the build with the flag at its default. Nothing changes.
2. Enable `--compact.block-split.max-shards` on a trial bucket. Confirm shards appear, that `thanos tools bucket verify` reports no overlaps, and that queries across the split range return the same results as before.
3. Remove existing `index-size-exceeding` markers with `thanos tools bucket mark --remove` for blocks that should be re-planned. They are split on the next cycle.
4. Enable on production compactors. Store gateways need the matching build first so the label is stripped; a store gateway without it would expose the label but still return correct data.
5. Optionally configure `--selector.relabel-config` on store gateways to spread shards.

Disabling the flag stops new splits. Existing shard groups keep compacting within themselves; they are valid blocks and need no cleanup.

## Alternatives

**Shard identity in `thanos.extensions` (Cortex).** Keeps labels clean, but requires a shard-aware metadata deduplication filter in both compactor and store gateway, a shard-aware grouper and planner so shards are not vertically merged back together, and shard-aware coverage logic in the resolution filter. Cortex accepted that work because its read path never exposes external labels. In Thanos the label approach removes all of it in exchange for one strip in the store gateway.

**Reactive splitting after an overflow (#4191).** Splits only once a compaction has failed or produced an oversized block, so at least one large compaction is wasted per split. Uses a fixed set of sixteen hash functions to get adaptivity. Power-of-two modulo gives the same adaptivity without the wasted attempt and with a trivial compatibility rule.

**Single-pass multi-output compaction (Mimir).** Writes all M shards while reading the inputs once. Requires the Prometheus fork Mimir maintains; upstream never merged it. It is the right optimisation once splitting exists and can be added behind the same populator interface later. It changes cost, not behaviour.

**64-bit postings upstream.** [prometheus/prometheus#5884](https://github.com/prometheus/prometheus/pull/5884) was closed unmerged in 2024. Even if it landed, the operational reasons for bounded block size remain.

**Keep marking, add better tooling.** Does not solve deduplication of oversized replicas, and leaves downsampling stuck.

## Action plan

Each step is independently mergeable and testable in standalone mode.

1. **Plan outputs in the seam.** A plan names its outputs - labels and an optional series partition - and the in-process executor produces them with `PartitionedBlockPopulator`, uploading every output before marking sources. Unit tests over synthetic blocks: every series lands in exactly one partition, symbols are per partition, deduplication within a partition matches a whole compaction, empty partitions produce no block. This lives in the [execution seam](https://github.com/frycm/thanos/pull/10), since it is the shape of a plan, not a splitting rule.
2. **Output planning.** The label constant and value format with parse and format helpers, the shard count function, and the split planner that names a plan's outputs. Test that a crash between uploads leaves the sources in place, and that the rerun's duplicates are removed by the deduplication filter and garbage collection.
3. **Planner wiring.** Do not install the index-size filter when splitting is enabled; keep fallback marking. Flags, metrics, docs.
4. **Store gateway.** Strip the shard label from series, label names and label values. End-to-end test: a split range returns the same series, samples and label names as the unsplit original, with and without replica deduplication.
5. **Invariant tests** in the deduplication filter, the resolution filter and the downsampling planner, each with two shards of one plan.
6. **Distributed compactor.** Verifier accepts the shard label; oversized check applies to inputs only. Then fan-out with per-shard tasks and journal bookkeeping.
7. **Later.** Query-time shard skipping via store hints, a merge-back tool, and a single-pass populator.

## Future work

* **Block skipping at query time.** Mimir's querier drops blocks whose compactor shard cannot contain the query shard when one shard count divides the other. Thanos could do the same for sharded queries through store request hints once query sharding by series hash exists.
* **Merge-back and re-shard tool.** A bucket tool that compacts `3_of_16` and `11_of_16` back into `3_of_8`, or all shards into one block, for operators who over-split.
* **Single-pass splitting.** A populator that writes M index and chunk writers in one pass over the inputs, removing the M-fold read cost in standalone mode.

## Open questions

* Should the shard label name be `__compactor_shard__` or reuse Mimir's `__compactor_shard_id__` so tooling that already understands Mimir blocks works unchanged? Reusing it is tempting; the value format `i_of_M` is already identical.
* Should M also be capped by a per-shard chunk size, given that chunk files, not the index, dominate object storage transfer time for some workloads?
* Is the 85% headroom still the right figure for a per-shard target, or should shards aim lower, for example 50%, to leave room for horizontal growth before the next re-split?

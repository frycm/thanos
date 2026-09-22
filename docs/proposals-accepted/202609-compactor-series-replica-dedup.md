---
type: proposal
title: Compactor deduplication of HA replicas whose replica label is inside the series
status: proposed
owner: frycm
menu: proposals-accepted
---

* **Owners:**
  * @frycm

* **Related Tickets:**
  * [#4239 Add penalty based deduplication mode for compactor](https://github.com/thanos-io/thanos/pull/4239)
  * [#4328 About HA prometheus instances with remote write](https://github.com/thanos-io/thanos/issues/4328)
  * [#8317 Vertical Compaction not working with OpenShift Monitoring remote-write due to missing prometheus_replica label in Receive blocks](https://github.com/thanos-io/thanos/issues/8317)
  * [#6004 Thanos compactor & receiver: vertical compaction not working as expected](https://github.com/thanos-io/thanos/issues/6004)
  * [#3871 Deduplication labels per tenant](https://github.com/thanos-io/thanos/issues/3871)
  * Proposal and discussion: [frycm/thanos#12](https://github.com/frycm/thanos/issues/12)

* **Other docs:**
  * [Compactor](../components/compact.md#deduplicating-replicas-whose-replica-label-is-inside-the-series)
  * [Block splitting by series](https://github.com/frycm/thanos/pull/11), which this composes with

> TL;DR: The compactor deduplicates HA replicas only when the replica label is an external label of the block. Behind a receiver, an agent or an OpenTelemetry collector, the replica label lands inside the series, and both copies of every series survive every compaction and downsampling pass; only the querier hides them, forever. `--deduplication.series-replica-label` makes every compaction merge series that differ only in the given labels, with the configured merge function, and drop the labels. In the bucket that motivated this, it halves the stored series.

## Why

`--deduplication.replica-label` names external labels. The compactor strips them from the block's labels so that replicas fall into one group, and vertical compaction merges the overlapping blocks. That works when each replica uploads its own blocks, as sidecars do. Behind Receive, both replicas of a Prometheus pair write into the same tenant; the receiver's replica label is external, the Prometheus replica label is an ordinary series label, and two series that differ only in it are two series to the whole storage path.

### Pitfalls of the current solution

* Storage, index size, downsampling work and store gateway memory are doubled for the life of the data. The 64 GiB index limit is reached at half the logical series count.
* Every query pays for the second copy and merges it in the querier.
* Block splitting makes the doubled stream fit; it does not make it smaller.

## Goals

* Series that differ only in configured series labels are merged at compaction and the labels are dropped.
* The read path is correct before, during and after the rollout, with old and new blocks side by side.
* The output depends on the source blocks alone.
* Opt-in, default off; composes with block splitting; nothing in the planner changes.

## Non-Goals

* Deduplicating at ingestion.
* Rewriting blocks that will never be compacted again; they turn over with retention.

## How

### A deduplicating populator

The execution seam produces what a plan names through a `tsdb.BlockPopulator`. `DeduplicatingBlockPopulator` mirrors `tsdb.DefaultBlockPopulator` with two differences: it presents every series without the replica labels, and it merges the series that then share their labels with the compactor's merge function. The symbol table is built from what it writes, so the dropped names and values leave the block.

**Order without materializing labels.** The index writer needs series in label order, and dropping a label does not preserve it: `{a="1", r="x"}` sorts before `{a="1", b="1", r="x"}`, but `{a="1"}` sorts after `{a="1", b="1"}`. The order only breaks between series with different label names, though. Among series with the same label names and the same replica values, the dropped labels sit at the same position with the same value in every comparison and never decide one. The populator therefore groups each source's postings by replica values and label names - roughly one group per metric family and replica - and each group is already in stripped order. The groups of one replica are merged into one postings list with a heap that reads labels from the index on demand. Memory is two series references per series, not one label set per series.

**A fixed order for the merge function.** The compactor's penalty merger (`pkg/dedup`) walks the replicas' chunks by time; each group of overlapping chunks is merged into the one that starts first, and the penalty algorithm keeps that base's samples where replicas have one at the same timestamp. Two things made the choice depend on something other than the data. `storage.NewMergeChunkSeriesSet` does not keep a stable order among equal series, so the populator merges its per-source, per-replica series sets itself and offers equal series in a fixed order: by source block in the plan's order, then by the replica labels compared as a label set, a series lacking them first. And the merger's heap ordered chunks by time only, so between chunks with the same time range - replicas scraped in lockstep, or blocks written by the same receiver - the base depended on how the heap had settled; it now breaks such ties by the position of the series, which also makes today's vertical compaction of external replicas deterministic. The scenario suite found this: a deduplicated series switched from one replica to the other in the one window whose other receiver copy was missing, which left raw samples valid but changed the cumulative counter aggregate of the downsampled block.

Where replica chunks do not align, the base is the chunk that starts first, as in today's vertical compaction, and the merged series may move from one replica to another between overlap groups. Query-time deduplication has no fixed order either - the querier strips replica labels and re-sorts with an unstable sort over the order responses arrived in - so where replicas disagree, the compactor and the querier may pick different replicas, as two queries already may. Every sample the compactor keeps is one a replica really has.

**Series lacking the labels** are replica "none" and merge with their replicas, so a late replica joins an already deduplicated series.

**Checks.** The populator refuses output that is not strictly increasing in stripped labels, which is what a broken grouping would produce. It writes a partition only if the partition leaves the replica labels out of its hash (`SeriesPartition.Without`); otherwise the replicas of one series would land in different shards, each shard would write the series under the same labels, and nothing could merge them again.

### Where the decision lives

Deduplication by series labels is a policy of the whole stream, like the external replica labels and the merge function, and needs nothing of the planner's bucket-wide view. It is configured on the executor, `LocalPlanExecutor.SeriesReplicaLabels`. With block splitting, the split planner's `--compact.block-split.ignore-labels` must name the same labels, and the populator enforces that per plan.

### Read path

The querier keeps its replica labels. Old blocks carry both replicas; deduplicated blocks carry one series without the label; a querier deduplicating by the same labels serves the same series at the same timestamps from both. Selectors and `label_values()` on the replica label return nothing for deduplicated ranges, and rules grouping by it see one series where they saw two; that is inherent to removing the label.

### Metadata and downsampling

A compaction that deduplicated records the labels as `thanos.series_replica_labels`; a downsampled block inherits it from its source. Nothing on the read path consults it. Downsampled blocks made from deduplicated raw blocks are deduplicated.

### Manager/worker mode

Workers run the same executor and need the same flag. The manager's lease-time check of deduplication settings has to include it; that belongs to the manager's PR and is a follow-up once both are merged.

### Configuration and observability

* `--deduplication.series-replica-label` (repeatable, default empty).
* `thanos_compact_series_dedup_input_series_total` and `thanos_compact_series_dedup_output_series_total`.

## Alternatives

1. **Deduplicate at the receiver.** Removes the duplication before it is stored, but changes the write path and cannot help data already stored.
2. **Query-time only, as today.** Correct and simple; pays the doubled cost everywhere, for the life of the data.
3. **A relabeling rewrite that drops the label.** Drops the label but does not merge the series; two series with identical labels in one block is a corrupt block.

## Validation

* Unit tests: the merger's tie-break by series position, whatever earlier chunks did to its heap; the order counter-example; replicas across two sources with the tie going to the first source's first replica, and to the other when the sources are swapped; a series without the label merging with its replica; the label leaving the symbol table; replicas with a gap, where penalty deduplication switches replica, matching the querier's penalty deduplication of the same series; partitions composing into the whole output and a partition hashing the replica label refused; the executor recording the labels and counting series.
* Scenario suite: the generic fault scenarios - object store outage, process crash, corrupted source, failed publications of every block file - with deduplication on, judged against the standalone compactor with it on; and `series_dedup_serves_what_query_time_dedup_served`, judging the result against a standalone run without deduplication as a querier deduplicating by the replica labels sees both.

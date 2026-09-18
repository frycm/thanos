# Distributed compactor failure coverage

The distributed compactor uses the same TSDB compaction and downsampling algorithms as standalone mode. Its additional correctness obligations are task ownership, complete publication, result validation, source deletion and recovery. Tests check those obligations at three levels: focused state-transition tests, fault injection around real TSDB blocks and HTTP interactions, and separate Thanos command processes using filesystem buckets.

## Deployment represented by the corpus

The fault scenarios use penalty deduplication configured with `prometheus_replica`, `receiver_replica`, `otelcol_replica` and `ruler_replica`. Prometheus and OTel replica labels occur inside series; receiver and ruler replicas occur in block external labels. The corpus includes an HA Prometheus pair received with RF=2, windows missing one receiver copy, collector replicas, ruler replicas, integer and float native histograms, and a tenant without HA.

Each scenario compares the served samples and downsampled aggregates against a standalone run on identical input. It also checks nonempty output, no unexpected block overlap, no live tasks after convergence, and recoverable source provenance. The process test runs actual standalone, manager and two worker command lifecycles and compares every served sample. These tests synthesize input blocks; they do not start Prometheus agents, receiver routers or OTel collectors.

## Failure matrix

| Failure or boundary                                                                                                                                    | Coverage                                                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Worker cancellation during metadata/chunk reads, compaction, ownership checks, uploads and checksum reads                                              | `TestWorkerShutdownAtExecutionStages`, graceful restart interaction                                                                                        |
| Worker crash and expired lease; manager restart; stale owner; mismatched worker configuration                                                          | Scheduler tests, interaction tests, fault scenarios                                                                                                        |
| Journal read/write outage and failed/lost result reports                                                                                               | Journal, scheduler, report-retry and interaction tests                                                                                                     |
| Chunk, index or metadata publication fails before writing or after a successful write loses its acknowledgement                                        | `TestWorkerPublicationFailuresPreserveData`: compaction, raw-to-5m and 5m-to-1h; immutable source bytes and sample/aggregate equality after retry          |
| Source deletion-mark write fails or its acknowledgement is lost                                                                                        | `TestSourceDeletionFailureCanBeRetried`: source bytes preserved, replacement remains readable, repeated finalization is idempotent                         |
| Invalid compaction result: checksum, provenance, labels, sources, resolution, source/time coverage, empty output                                       | Verification and finalization tests; no source deletion before validation                                                                                  |
| Rejected result left in the bucket: metadata sync and garbage collection afterwards, delete refused, journal write refused                             | `TestRejectedOutputsNeverRetireTheSources`: rejected blocks deleted or recorded and unpublished through sync and GC; halt when neither is possible         |
| Completed report never verified (manager stopped between report and verification); an output the plan named missing from a completed report            | `TestMaintenanceRejectsUnverifiedOutputs`, `TestClaimOutputs`, `TestOutputPublished`: outputs supersede nothing until verified, every output accounted for |
| Invalid downsampling result: empty/duplicate output, mismatched block ID, labels, sources, times, resolution, provenance, checksum or missing metadata | `TestDispatchDownsamplingRejectsInvalidResults` through the dispatcher                                                                                     |
| Source index corruption, terminal data errors, oversized/abandoned tasks                                                                               | Interaction tests, error classification and parked-task scenarios                                                                                          |
| Parallel plans for a vertical group with mixed block sizes                                                                                             | Real-planner tests at raw, 5m and 1h resolutions, plus fault-scenario content comparison                                                                   |
| Rollback after completion, during a run, after GC marks or interruption; missing sources or foreign marks                                              | Rollback unit and interaction tests, fault scenarios                                                                                                       |
| CLI defaults, manager/worker communication and process lifecycle                                                                                       | `TestCompactorManagerWorkerProcesses`                                                                                                                      |

## Running the checks

Use the repository's `slicelabels` tag:

```bash
go test -tags slicelabels -race -timeout 15m ./pkg/compact/distributed -count=1 -args -distributed.scenarios
go test -tags slicelabels -race -timeout 5m ./cmd/thanos -run TestCompactorManagerWorkerProcesses -count=1
```

The Go CI workflow explicitly runs both commands with Go 1.25 and Go 1.26.

The corpus, the content oracle, the fault-injectable bucket views and the compactor process come from `pkg/compact/compacttest`, which also holds the scenarios every compactor has to survive whatever executes its plans: an object store outage, a process crash and restart, a corrupted source, and failed publications of every block file. `TestGenericScenarios` runs those against a manager with two workers; `TestScenarios` adds the failures only the distributed compactor can have. The same generic scenarios run against the standalone compactor in `pkg/compact/compacttest` itself. Ordinary unit tests also run the publication and result-validation regressions; the larger fault-scenario corpus remains opt-in outside that dedicated CI job.

## What the tests do not guarantee

Finite tests cannot enumerate every failure timing or prove corruption impossible. The supported operating conditions still matter:

- One active manager per shard; journal ownership checks are not an atomic distributed lock. Concurrent unrelated writers or operator edits are outside this coordination protocol.
- Object storage must provide its normal complete-object publication and durable-read guarantees. These tests inject API failures and lost acknowledgements; they do not certify a provider's implementation or protect against arbitrary storage corruption after a successful upload.
- Rollback requires all writers stopped and original source files still present. It cannot recover sources already physically removed by retention or GC.
- Filesystem and in-memory testing does not replace a trial against the deployed provider, bucket cache configuration, Kubernetes lifecycle and production data.

Use the [trial procedure](distributed-compaction-trial.md) before production cutover. Query old and new data through the production-style store/query path, including counter resets, replica gaps, native histograms if used and any backfill/out-of-order ingestion configuration.

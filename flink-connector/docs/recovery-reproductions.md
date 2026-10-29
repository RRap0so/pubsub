# Checkpoint and replay reproductions

These tests characterize the connector's recovery behavior. A passing test asserts
the documented outcome, including remaining missing-input and duplicate-state
scenarios. In-flight redelivery within one reader is now suppressed until
checkpoint acknowledgement, using the latest delivery's acknowledgement handle.

## Run locally

Requirements: JDK 17, Maven, Python 3, curl, and the Google Cloud Pub/Sub emulator.
No cloud project, credentials, or Docker daemon is required.

From `flink-connector/`:

```sh
bash run-recovery-tests.sh
```

The script starts an isolated emulator on a free loopback port, runs the module's
unit tests plus the integration tests, and stops its emulator on exit. It prints
the emulator log location. Maven reports are written to
`flink-connector-gcp-pubsub/target/surefire-reports/`.

To use an already running emulator:

```sh
CHECKPOINT_TEST_EMULATOR=127.0.0.1:8085 bash run-recovery-tests.sh
```

Both test classes skip when `CHECKPOINT_TEST_EMULATOR` is absent. The local runner
sets it and includes `PubSubRecoveryIT` explicitly; ordinary unit test runs do not
require an emulator.

## Coverage

`PubSubRecoveryIT` runs real Flink jobs in a MiniCluster, with managed operator
state and the actual Pub/Sub source connected to the emulator:

| Scenario | Observed outcome |
| --- | --- |
| Process A, take a savepoint, process and checkpoint B, cancel, restore the older savepoint, publish C | State becomes A,C: B is absent from restored state and is not automatically replayed. |
| Checkpoint A, fail once while processing B, allow Flink to restart | Flink restores A and receives B again, producing A,B. |
| Process A without a completed checkpoint, take a Pub/Sub snapshot, stop with a savepoint, seek to the snapshot, restore | A is replayed into state that already contains A, producing A,A. |
| Enable replay snapshots, checkpoint A, take an older savepoint, checkpoint B, cancel, restore the savepoint | Previously acknowledged B is replayed. Tests cover one and two source readers. Duplicate input is permitted. |

`PubSubCheckpointRecoveryEmulatorTest` drives real source-reader checkpoint
callbacks against the emulator, with serialized split state and a mocked reader
context. It does not run a Flink coordinator:

| Scenario | Observed outcome |
| --- | --- |
| Fail before the next checkpoint completes | Unacknowledged work is redelivered. |
| Restore an older checkpoint after a newer checkpoint acknowledges input | Later acknowledged work is not replayed; a newly published control record still arrives. |
| Persist a checkpoint but lose the source's completion notification | Already checkpointed work is redelivered. |
| Set the outstanding-message limit to one and delay checkpoint completion | Input stops after one message; taking a checkpoint alone does not release capacity; successful completion does. |
| Disable automatic lease extension and delay checkpoint completion | Pub/Sub redelivers the message, but the reader emits it once and acknowledges the latest delivery when the checkpoint completes. |
| Keep automatic lease extension enabled while delaying a checkpoint beyond the subscription deadline | No duplicate is observed during the controlled delay. |
| Restore a replay-enabled checkpoint whose snapshot is missing | Restore fails before readers start instead of silently skipping input. |

`PubSubReplaySnapshotsTest` covers reference serialization, retained and in-flight
boundaries, intermediate savepoints, failed create/delete RPCs, aborted
checkpoints, namespace isolation, and orphan cleanup during recovery.

All test resources use unique synthetic names and are removed during cleanup.
Test endpoints must be loopback addresses.

## Limits

The emulator does not model production snapshot storage charges or configurable
retention accurately. These tests do not measure billing, high-volume throughput,
client lease-estimator convergence, or Kubernetes JobManager/HA behavior. They
also do not establish exactly-once delivery or end-to-end sink semantics.

In-flight suppression is local to each reader and is cleared after checkpoint
acknowledgement or shutdown. It does not deduplicate across readers, restarts,
snapshot seeks, or deliveries arriving after the checkpoint acknowledgement.

The manual snapshot test deliberately demonstrates the acknowledgement-state
mismatch independently of the opt-in checkpoint replay implementation below.

## Opt-in checkpoint replay

To retain replay boundaries for future checkpoints and savepoints:

```java
PubSubSource.<String>builder()
    .setProjectName("project")
    .setSubscriptionName("dedicated-subscription")
    .setDeserializationSchema(
        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
    .setReplaySnapshotRetentionCount(3)
    .build();
```

This mode is disabled by default. The coordinator creates its Pub/Sub snapshot
before Flink triggers the readers' checkpoint barriers, records the reference in
enumerator state, and seeks to that reference on restore before assigning input.
Acknowledgements still wait for successful Flink checkpoints.

The count retains the most recent snapshot boundaries at or before the newest
completed checkpoint, including intermediate savepoints, plus in-flight
boundaries. It is a **bounded replay window**, not indefinite savepoint retention.
The coordinator refuses new boundaries once it tracks `count + 1` snapshots;
configure one concurrent checkpoint and let checkpoints complete between manual
savepoints. It fails rather than growing an unbounded set when checkpoints stop
completing. The source's serialized namespace also lets a fresh retry reclaim
uncheckpointed resources from its earlier attempt.
Restoring a pruned or expired snapshot fails. Checkpoints made without this mode
cannot acquire replay protection retroactively. Disabling the option when
restoring a protected checkpoint is rejected. The serializer accepts legacy
version-zero state, but older connector binaries do not support this replay mode.

Use a subscription dedicated to one Flink job. Stop the original job and all
other subscribers before restoring; seeking changes subscription-wide state.
The job needs permissions to create, delete, list, and seek snapshots. Create or
seek failures fail the operation. Failed deletions remain queued and block new
snapshot creation until cleanup succeeds. Restore reconciles its persisted
namespace before creating new snapshots, preserving the selected boundary and
retained history without touching other namespaces. This also retries deletion
failures across restarts. Restoring an older checkpoint discards later snapshots
from the superseded execution; those later checkpoints cannot then be used to
restore this source.

**Pub/Sub charges for retained snapshot data.** The count bounds active snapshot
history, not retained bytes or total cost. Snapshots intentionally survive job
shutdown for recovery; abandoned jobs need independent cleanup, and Pub/Sub's
own expiration still applies. No snapshot resources are created by the default
configuration. The local emulator does not validate billing or expiration.

Replay provides at-least-once input recovery. It can replay a record already in
the restored Flink state, so it does not fix the duplicate-state reproduction or
provide Kafka's exactly-once source semantics.

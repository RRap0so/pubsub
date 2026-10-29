# Checkpoint and replay reproductions

These tests characterize the connector's existing recovery behavior. They add no
connector behavior changes. A passing reproduction asserts the observed outcome,
including missing input or duplicate state updates in the scenarios below.

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

`PubSubCheckpointRecoveryEmulatorTest` drives real source-reader checkpoint
callbacks against the emulator, with serialized split state and a mocked reader
context. It does not run a Flink coordinator:

| Scenario | Observed outcome |
| --- | --- |
| Fail before the next checkpoint completes | Unacknowledged work is redelivered. |
| Restore an older checkpoint after a newer checkpoint acknowledges input | Later acknowledged work is not replayed; a newly published control record still arrives. |
| Persist a checkpoint but lose the source's completion notification | Already checkpointed work is redelivered. |
| Set the outstanding-message limit to one and delay checkpoint completion | Input stops after one message; taking a checkpoint alone does not release capacity; successful completion does. |
| Disable automatic lease extension and delay checkpoint completion | The source emits the same message more than once before any checkpoint completes. |
| Keep automatic lease extension enabled while delaying a checkpoint beyond the subscription deadline | No duplicate is observed during the controlled delay. |

All test resources use unique synthetic names and are removed during cleanup.
Test endpoints must be loopback addresses.

## Limits

The emulator does not model production snapshot storage charges or configurable
retention accurately. These tests do not measure billing, high-volume throughput,
client lease-estimator convergence, or Kubernetes JobManager/HA behavior. They
also do not establish exactly-once delivery or end-to-end sink semantics.

The snapshot test deliberately demonstrates the acknowledgement-state mismatch;
it does not introduce a snapshot-based recovery implementation.

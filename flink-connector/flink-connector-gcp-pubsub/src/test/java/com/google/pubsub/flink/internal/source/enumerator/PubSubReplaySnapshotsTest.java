/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.flink.internal.source.enumerator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.Test;

public class PubSubReplaySnapshotsTest {
  private final MemoryStore store = new MemoryStore();

  @Test
  public void snapshot_serializesReplayReferenceAndOwnership() throws Exception {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint checkpoint = snapshot(manager, 1);
    assertThat(checkpoint.getReplaySnapshotNamespace()).matches("[0-9a-f]{32}");
    assertThat(checkpoint.getReplaySnapshotsCount()).isEqualTo(1);
    assertThat(checkpoint.getReplaySnapshots(0).getName())
        .startsWith("projects/project/snapshots/flink-");
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint)))
        .isEqualTo(checkpoint);
  }

  @Test
  public void restore_seeksToCheckpointReference() {
    PubSubEnumeratorCheckpoint checkpoint = snapshot(new PubSubReplaySnapshots(store, 2), 1);
    PubSubReplaySnapshots.restore(store, 2, checkpoint);
    assertThat(store.seeks).containsExactly(name(checkpoint));
  }

  @Test
  public void restore_missingSnapshotFailsInsteadOfContinuing() {
    PubSubEnumeratorCheckpoint checkpoint = snapshot(new PubSubReplaySnapshots(store, 2), 1);
    store.resources.clear();
    assertThrows(
        IllegalStateException.class, () -> PubSubReplaySnapshots.restore(store, 2, checkpoint));
  }

  @Test
  public void restore_legacyCheckpointWithoutReplayReferenceFails() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubReplaySnapshots.restore(
                store, 2, PubSubEnumeratorCheckpoint.getDefaultInstance()));
  }

  @Test
  public void completion_retainsLatestCompletedAndSharedPendingBoundary() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    PubSubEnumeratorCheckpoint third = snapshot(manager, 3);
    assertThat(name(third)).isEqualTo(name(second));
    assertThat(store.resources).containsExactly(name(first), name(second));
    manager.notifyCheckpointComplete(2);
    assertThat(name(snapshot(manager, 3))).isEqualTo(name(third));
    PubSubEnumeratorCheckpoint fourth = snapshot(manager, 4);
    assertThat(store.resources).containsExactly(name(second), name(fourth));
    manager.notifyCheckpointComplete(
        1); // Late notification must not regress the protected boundary.
    assertThat(store.resources).containsExactly(name(second), name(fourth));
    manager.notifyCheckpointComplete(4);
    assertThat(store.resources).containsExactly(name(fourth));
  }

  @Test
  public void incompleteCheckpoints_shareOneBoundaryWithoutAdditionalCreates() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    assertThat(name(snapshot(manager, 2))).isEqualTo(name(first));
    assertThat(name(snapshot(manager, 3))).isEqualTo(name(first));
    PubSubEnumeratorCheckpoint fourth = snapshot(manager, 4);
    assertThat(name(fourth)).isEqualTo(name(first));
    assertThat(store.creates).isEqualTo(1);
    assertThat(store.resources).containsExactly(name(first));
    manager.notifyCheckpointComplete(3);
    assertThat(snapshot(manager, 4)).isEqualTo(fourth);
    PubSubEnumeratorCheckpoint fifth = snapshot(manager, 5);
    assertThat(store.resources).containsExactly(name(first), name(fifth));
    assertThat(store.creates).isEqualTo(2);
  }

  @Test
  public void freshRestart_preservesUnknownSnapshotsWithinStableNamespace() {
    String namespace = "00000000000000000000000000000000";
    PubSubReplaySnapshots firstAttempt = PubSubReplaySnapshots.startFresh(store, 1, namespace);
    PubSubEnumeratorCheckpoint existing = snapshot(firstAttempt, 1);
    snapshot(firstAttempt, 2);
    PubSubEnumeratorCheckpoint unrelated = snapshot(new PubSubReplaySnapshots(store, 1), 1);
    PubSubReplaySnapshots.startFresh(store, 1, namespace);
    assertThat(store.resources).containsExactly(name(existing), name(unrelated));
  }

  @Test
  public void completion_preservesIntermediateSavepointWithinRetentionWindow() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint savepoint = snapshot(manager, 2); // No completion notification.
    PubSubEnumeratorCheckpoint next = snapshot(manager, 3);
    assertThat(name(next)).isEqualTo(name(savepoint));
    manager.notifyCheckpointComplete(3);
    assertThat(store.resources).containsExactly(name(first), name(savepoint));
  }

  @Test
  public void fullRetentionWindow_savepointWithoutCompletionDoesNotBlockNextCheckpoint() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    snapshot(manager, 2);
    manager.notifyCheckpointComplete(2);
    snapshot(manager, 3); // A completed savepoint need not send a completion notification.

    // The next periodic checkpoint must be able to complete and advance retention. Otherwise
    // every future checkpoint hits the capacity guard, so ACK-on-checkpoint input stalls forever.
    PubSubEnumeratorCheckpoint next = snapshot(manager, 4);

    assertThat(store.resources.size()).isAtMost(3);
    manager.notifyCheckpointComplete(4);
    assertThat(store.resources).contains(name(next));
    assertThat(store.resources.size()).isAtMost(2);
    PubSubReplaySnapshots.restore(store, 2, next);
    assertThat(store.seeks).containsExactly(name(next));
  }

  @Test
  public void manyUnnotifiedSavepoints_keepStorageBoundedAndAllowCheckpointCompletion()
      throws Exception {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    snapshot(manager, 2);
    manager.notifyCheckpointComplete(2);
    PubSubEnumeratorCheckpoint savepoint = snapshot(manager, 3);
    PubSubEnumeratorCheckpoint next = savepoint;
    for (long id = 4; id <= 100; id++) {
      next = snapshot(manager, id);
      assertThat(name(next)).isEqualTo(name(savepoint));
      assertThat(store.resources).hasSize(3);
    }
    assertThat(store.creates).isEqualTo(3);
    manager.notifyCheckpointComplete(100);
    assertThat(store.resources).hasSize(2);
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    PubSubReplaySnapshots restored =
        PubSubReplaySnapshots.restore(
            store, 2, serializer.deserialize(serializer.getVersion(), serializer.serialize(next)));
    assertThat(store.seeks).containsExactly(name(savepoint));
    PubSubEnumeratorCheckpoint fresh = snapshot(restored, 101);
    assertThat(name(fresh)).isNotEqualTo(name(savepoint));
    assertThat(store.creates).isEqualTo(4);
    assertThat(store.resources).hasSize(3);
  }

  @Test
  public void abortCreator_preservesBoundaryForDependentCheckpointAndLateAborts() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint creator = snapshot(manager, 1);
    PubSubEnumeratorCheckpoint dependent = snapshot(manager, 2);

    manager.notifyCheckpointAborted(1);
    assertThat(store.resources).containsExactly(name(creator));
    manager.notifyCheckpointComplete(2);
    manager.notifyCheckpointAborted(1);
    manager.notifyCheckpointAborted(2);

    assertThat(store.resources).containsExactly(name(dependent));
    PubSubReplaySnapshots.restore(store, 1, dependent);
    assertThat(store.seeks).containsExactly(name(creator));
  }

  @Test
  public void abortLastDependent_deletesOnlyUncommittedBoundary() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    snapshot(manager, 2);
    manager.notifyCheckpointAborted(1);
    manager.notifyCheckpointAborted(2);
    assertThat(store.resources).isEmpty();

    PubSubEnumeratorCheckpoint next = snapshot(manager, 3);
    assertThat(name(next)).isNotEqualTo(name(first));
    assertThat(store.resources).containsExactly(name(next));
  }

  @Test
  public void repeatedSnapshot_keepsItsSelectionWhenOlderCheckpointCompletes() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    manager.notifyCheckpointComplete(1);
    snapshot(manager, 3);

    assertThat(snapshot(manager, 2)).isEqualTo(second);
    assertThat(name(second)).isEqualTo(name(first));
    assertThat(store.creates).isEqualTo(2);
  }

  @Test
  public void outOfOrderSnapshot_neverSelectsFutureBoundary() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint future = snapshot(manager, 3);
    assertThrows(IllegalArgumentException.class, () -> snapshot(manager, 2));
    assertThat(store.resources).containsExactly(name(future));
    assertThat(store.creates).isEqualTo(1);
  }

  @Test
  public void abortedCheckpoint_deletesOnlyItsOwnSnapshot() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    snapshot(manager, 2);
    manager.notifyCheckpointAborted(2);
    assertThat(store.resources).containsExactly(name(first));
  }

  @Test
  public void failedDeletion_isRetriedBeforeAnyNewSnapshotCanBeCreated() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    store.failedDeletions.add(name(first));
    manager.notifyCheckpointComplete(2);
    int creates = store.creates;
    assertThat(name(snapshot(manager, 3))).isEqualTo(name(second));
    assertThat(store.creates).isEqualTo(creates);
    store.failedDeletions.clear();
    PubSubEnumeratorCheckpoint third = snapshot(manager, 4);
    assertThat(store.resources).containsExactly(name(second), name(third));
  }

  @Test
  public void failedCreate_cleansUpPossibleRemoteSuccessBeforeTryingAgain() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    store.failAfterCreate = true;
    assertThrows(IllegalStateException.class, () -> snapshot(manager, 1));
    assertThat(store.resources).hasSize(1);
    store.failAfterCreate = false;
    PubSubEnumeratorCheckpoint next = snapshot(manager, 2);
    assertThat(store.resources).containsExactly(name(next));
  }

  @Test
  public void recovery_preservesNewerBoundariesAndOtherNamespaces() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint checkpoint = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint orphan = snapshot(original, 2);
    original.notifyCheckpointComplete(2);
    PubSubEnumeratorCheckpoint unrelated = snapshot(new PubSubReplaySnapshots(store, 1), 1);
    PubSubReplaySnapshots restored = PubSubReplaySnapshots.restore(store, 2, checkpoint);
    assertThat(store.resources).contains(name(orphan));
    assertThat(store.resources).contains(name(checkpoint));
    PubSubEnumeratorCheckpoint next = snapshot(restored, 3);
    restored.notifyCheckpointComplete(3);
    assertThat(store.resources)
        .containsExactly(name(checkpoint), name(orphan), name(next), name(unrelated));
  }

  @Test
  public void recovery_retriesFailedDeletionBeforeCreatingAnythingAfterRestart() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(original, 2);
    store.failedDeletions.add(name(first));
    original.notifyCheckpointComplete(2);
    int creates = store.creates;

    PubSubReplaySnapshots.restore(store, 1, second);
    assertThat(store.creates).isEqualTo(creates);
    assertThat(store.resources).containsExactly(name(first), name(second));
    store.failedDeletions.clear();
    PubSubReplaySnapshots.restore(store, 1, second);
    assertThat(store.resources).containsExactly(name(second));
  }

  @Test
  public void close_keepsRecoverySnapshotsAndClosesClient() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint checkpoint = snapshot(manager, 1);
    manager.close();
    assertThat(store.resources).containsExactly(name(checkpoint));
    assertThat(store.closed).isTrue();
  }

  @Test
  public void repeatedRecovery_keepsRetentionBoundedAndEverySelectedSnapshotRestorable()
      throws Exception {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    for (long id = 1; id <= 200; id++) {
      PubSubEnumeratorCheckpoint checkpoint = snapshot(manager, id);
      // Every persisted reference must exist at the checkpoint boundary.
      for (var reference : checkpoint.getReplaySnapshotsList()) {
        assertThat(store.resources).contains(reference.getName());
      }
      assertThat(store.resources.size()).isAtMost(3);
      manager.notifyCheckpointComplete(id);
      assertThat(store.resources.size()).isAtMost(2);
      if (id % 5 == 0) {
        manager.close();
        checkpoint =
            serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));
        manager = PubSubReplaySnapshots.restore(store, 2, checkpoint);
        assertThat(store.seeks.get(store.seeks.size() - 1)).isEqualTo(name(checkpoint));
        assertThat(store.resources).contains(name(checkpoint));
        assertThat(store.resources.size()).isAtMost(2);
      }
    }
    assertThat(store.creates).isEqualTo(200);
    assertThat(store.seeks).hasSize(40);
  }

  @Test
  public void failedSeek_doesNotDeleteAnySnapshotOrCreateReplacementBoundary() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint checkpoint = snapshot(manager, 1);
    snapshot(manager, 2);
    Set<String> before = new LinkedHashSet<>(store.resources);
    int creates = store.creates;
    store.failSeek = true;

    assertThrows(
        IllegalStateException.class, () -> PubSubReplaySnapshots.restore(store, 2, checkpoint));

    assertThat(store.resources).containsExactlyElementsIn(before);
    assertThat(store.creates).isEqualTo(creates);
    store.failSeek = false;
    PubSubReplaySnapshots.restore(store, 2, checkpoint);
    assertThat(store.resources).containsExactlyElementsIn(before);
  }

  @Test
  public void failoverToCheckpointSix_preservesAndRestoresSavepointSeven() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint six = snapshot(original, 6);
    original.notifyCheckpointComplete(6);
    PubSubEnumeratorCheckpoint seven = snapshot(original, 7);
    PubSubReplaySnapshots.restore(store, 2, six);
    assertThat(store.resources).contains(name(seven));
    PubSubReplaySnapshots.restore(store, 2, seven);
    assertThat(store.seeks).containsExactly(name(six), name(seven)).inOrder();
  }

  @Test
  public void deleteFailure_doesNotFailCompletionAndKeepsResourceBudgetBounded() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    store.failedDeletions.add(name(first));
    manager.notifyCheckpointComplete(2);
    for (int id = 3; id < 20; id++) {
      assertThat(name(snapshot(manager, id))).isEqualTo(name(second));
      manager.notifyCheckpointComplete(id);
      assertThat(store.resources).hasSize(2);
    }
    store.failedDeletions.clear();
    assertThat(name(snapshot(manager, 20))).isNotEqualTo(name(second));
  }

  @Test
  public void createFailure_reusesValidBoundarySoCheckpointCanComplete() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    store.failAfterCreate = true;
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    assertThat(name(second)).isEqualTo(name(first));
    manager.notifyCheckpointComplete(2);
    store.failAfterCreate = false;
    assertThat(name(snapshot(manager, 3))).isNotEqualTo(name(first));
  }

  @Test
  public void expiredBoundary_isNotUsedAsCreateFailureFallback() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    store.expired.add(name(first));
    store.failAfterCreate = true;
    assertThrows(IllegalStateException.class, () -> snapshot(manager, 2));
  }

  @Test
  public void preservedUnknownSavepoints_cannotGrowPastNamespaceBudgetAcrossRestarts() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint later = snapshot(original, 2);
    for (int id = 3; id < 30; id++) {
      PubSubReplaySnapshots recovered = PubSubReplaySnapshots.restore(store, 1, first);
      assertThat(name(snapshot(recovered, id))).isEqualTo(name(first));
      recovered.notifyCheckpointComplete(id);
      assertThat(store.resources).containsExactly(name(first), name(later));
    }
  }

  @Test
  public void failureAndExpiryMetrics_reportQueuedCleanupAndFallbackBoundary() {
    Map<String, Counter> counters = new HashMap<>();
    Map<String, Gauge<?>> gauges = new HashMap<>();
    var group =
        new UnregisteredMetricsGroup() {
          @Override
          public <C extends Counter> C counter(String name, C counter) {
            counters.put(name, counter);
            return counter;
          }

          @Override
          public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
          }
        };
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    manager.registerMetrics(group);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    store.expiries.put(name(second), System.currentTimeMillis() + 60000);
    store.failedDeletions.add(name(first));
    manager.notifyCheckpointComplete(2);
    assertThat(counters.get("snapshotDeleteFailures").getCount()).isEqualTo(1);
    assertThat(gauges.get("pendingSnapshotDeletions").getValue()).isEqualTo(1);
    snapshot(manager, 3);
    long remaining = (Long) gauges.get("replaySnapshotTimeToExpiryMillis").getValue();
    assertThat(remaining).isGreaterThan(0L);
    assertThat(remaining).isAtMost(60000L);
    store.failedDeletions.clear();
    store.failAfterCreate = true;
    snapshot(manager, 4);
    assertThat(counters.get("snapshotCreateFailures").getCount()).isEqualTo(1);
  }

  @Test
  public void oldBacklog_failedPreconditionCanReuseBoundaryAndCompleteCheckpoint() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    store.createFailure =
        new FailedPreconditionException(
            "Backlog too old for a new snapshot",
            null,
            GrpcStatusCode.of(Status.Code.FAILED_PRECONDITION),
            false);
    PubSubEnumeratorCheckpoint next = snapshot(manager, 2);
    assertThat(name(next)).isEqualTo(name(first));
    manager.notifyCheckpointComplete(2);
    assertThat(store.resources).containsExactly(name(first));
  }

  @Test
  public void oldBacklogWithoutAnyBoundary_failsInsteadOfClaimingReplayProtection() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    store.createFailure =
        new FailedPreconditionException(
            "Backlog too old for a new snapshot",
            null,
            GrpcStatusCode.of(Status.Code.FAILED_PRECONDITION),
            false);
    assertThrows(FailedPreconditionException.class, () -> snapshot(manager, 1));
    assertThat(store.resources).isEmpty();
  }

  @Test
  public void missingBoundary_isNotUsedAsCreateFailureFallback() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    store.resources.remove(name(first));
    store.createFailure = new IllegalStateException("Create failed");
    assertThrows(IllegalStateException.class, () -> snapshot(manager, 2));
  }

  @Test
  public void checkpointAllocation_listsNamespaceOnlyOnce() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    for (int id = 1; id <= 20; id++) {
      snapshot(manager, id);
      manager.notifyCheckpointComplete(id);
    }
    assertThat(store.listCalls).isEqualTo(1);
  }

  @Test
  public void rollbackFullBudget_reusesOldBoundaryAndCanRefreshAfterItsExpiry() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint six = snapshot(original, 6);
    original.notifyCheckpointComplete(6);
    PubSubEnumeratorCheckpoint seven = snapshot(original, 7);
    PubSubReplaySnapshots recovered = PubSubReplaySnapshots.restore(store, 1, six);
    int before = store.creates;
    for (int id = 8; id <= 27; id++) {
      assertThat(name(snapshot(recovered, id))).isEqualTo(name(six));
      recovered.notifyCheckpointComplete(id);
    }
    assertThat(store.creates).isEqualTo(before);
    store.expired.add(name(six));
    PubSubEnumeratorCheckpoint fresh = snapshot(recovered, 28);
    assertThat(name(fresh)).isNotEqualTo(name(six));
    assertThat(store.resources).containsExactly(name(seven), name(fresh));
  }

  @Test
  public void persistedPendingDeletion_isRetriedAfterRestart() throws Exception {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    store.failAfterCreate = true;
    PubSubEnumeratorCheckpoint second = snapshot(original, 2);
    String ambiguous =
        store.resources.stream()
            .filter(value -> !value.equals(name(first)))
            .findFirst()
            .orElseThrow();
    store.failedDeletions.add(ambiguous);
    original.notifyCheckpointComplete(2);
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    second = serializer.deserialize(serializer.getVersion(), serializer.serialize(second));
    store.failAfterCreate = false;
    PubSubReplaySnapshots restored = PubSubReplaySnapshots.restore(store, 1, second);
    assertThat(store.resources).contains(ambiguous);
    PubSubEnumeratorCheckpoint third = snapshot(restored, 3);
    third = serializer.deserialize(serializer.getVersion(), serializer.serialize(third));
    assertThat(third.getPendingSnapshotDeletionsCount()).isEqualTo(1);
    store.failedDeletions.clear();
    PubSubReplaySnapshots.restore(store, 1, third);
    assertThat(store.resources).doesNotContain(ambiguous);
    assertThat(store.resources).contains(name(first));
  }

  @Test
  public void rollbackExpiredBoundary_cannotBeRestored() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint six = snapshot(original, 6);
    original.notifyCheckpointComplete(6);
    snapshot(original, 7);
    PubSubReplaySnapshots recovered = PubSubReplaySnapshots.restore(store, 1, six);
    PubSubEnumeratorCheckpoint reused = snapshot(recovered, 8);
    assertThat(name(reused)).isEqualTo(name(six));
    recovered.notifyCheckpointComplete(8);
    store.expired.add(name(six));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class, () -> PubSubReplaySnapshots.restore(store, 1, reused));
    assertThat(failure).hasMessageThat().contains("missing or expired");
  }

  @Test
  public void ambiguousCreateBeforeDurableCheckpoint_isPreservedAsUnknownOnRestore() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint durable = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    store.failAfterCreate = true;
    snapshot(original, 2); // A crash occurs before this state becomes durable.
    String ambiguous =
        store.resources.stream()
            .filter(value -> !value.equals(name(durable)))
            .findFirst()
            .orElseThrow();
    store.failAfterCreate = false;

    PubSubReplaySnapshots recovered = PubSubReplaySnapshots.restore(store, 1, durable);
    assertThat(store.resources).containsExactly(name(durable), ambiguous);
    int creates = store.creates;
    assertThat(name(snapshot(recovered, 3))).isEqualTo(name(durable));
    assertThat(store.creates).isEqualTo(creates);
    assertThat(store.resources).contains(ambiguous);
  }

  @Test
  public void restore_rejectsUnsafePendingDeletionsBeforeSeekingOrDeleting() {
    PubSubEnumeratorCheckpoint first = snapshot(new PubSubReplaySnapshots(store, 2), 1);
    String namespace = first.getReplaySnapshotNamespace();
    String candidate = "flink-" + namespace + "-" + "a".repeat(32);
    for (String unsafe :
        List.of(
            name(first), // A full or short alias of a retained boundary must never be deleted.
            name(first).substring(name(first).lastIndexOf('/') + 1),
            "projects/other/snapshots/" + candidate,
            "projects/project/snapshots/flink-" + "b".repeat(32) + "-" + "c".repeat(32),
            "projects/project/snapshots/" + candidate + "/extra",
            "")) {
      PubSubEnumeratorCheckpoint corrupt =
          first.toBuilder().addPendingSnapshotDeletions(unsafe).build();
      assertThrows(
          IllegalArgumentException.class, () -> PubSubReplaySnapshots.restore(store, 2, corrupt));
      assertThat(store.seeks).isEmpty();
      assertThat(store.resources).containsExactly(name(first));
    }
  }

  @Test
  public void failedRestoreSeek_doesNotResolvePersistedPendingDeletions() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    String candidate =
        "projects/project/snapshots/flink-"
            + first.getReplaySnapshotNamespace()
            + "-"
            + "d".repeat(32);
    store.resources.add(candidate);
    PubSubEnumeratorCheckpoint checkpoint =
        first.toBuilder().addPendingSnapshotDeletions(candidate).build();
    store.failSeek = true;
    assertThrows(
        IllegalStateException.class, () -> PubSubReplaySnapshots.restore(store, 2, checkpoint));
    assertThat(store.resources).containsExactly(name(first), candidate);
    store.failSeek = false;
    PubSubReplaySnapshots.restore(store, 2, checkpoint);
    assertThat(store.resources).containsExactly(name(first));
  }

  @Test
  public void reviewedOfflineCleanupAndRestart_refreshesNamespaceBudget() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint newer = snapshot(original, 2);
    PubSubReplaySnapshots recovered = PubSubReplaySnapshots.restore(store, 1, first);
    assertThat(name(snapshot(recovered, 3))).isEqualTo(name(first));
    // The operator stops the coordinator, abandons the newer recovery point, and reviews cleanup.
    recovered.close();
    store.delete(name(newer));
    int listsBeforeRestart = store.listCalls;
    PubSubReplaySnapshots restarted = PubSubReplaySnapshots.restore(store, 1, first);
    assertThat(store.listCalls).isEqualTo(listsBeforeRestart + 1);
    PubSubEnumeratorCheckpoint fresh = snapshot(restarted, 4);
    assertThat(name(fresh)).isNotEqualTo(name(first));
    assertThat(store.listCalls).isEqualTo(listsBeforeRestart + 1);
  }

  private static PubSubEnumeratorCheckpoint snapshot(PubSubReplaySnapshots manager, long id) {
    PubSubEnumeratorCheckpoint.Builder checkpoint = PubSubEnumeratorCheckpoint.newBuilder();
    manager.snapshot(id, checkpoint);
    return checkpoint.build();
  }

  private static String name(PubSubEnumeratorCheckpoint checkpoint) {
    return checkpoint.getReplaySnapshotsList().stream()
        .filter(snapshot -> snapshot.getCheckpointId() == checkpoint.getReplayCheckpointId())
        .findFirst()
        .get()
        .getName();
  }

  private static class MemoryStore implements PubSubReplaySnapshots.Store {
    final Set<String> resources = new LinkedHashSet<>();
    final Set<String> failedDeletions = new HashSet<>();
    final Set<String> expired = new HashSet<>();
    final Map<String, Long> expiries = new HashMap<>();
    final List<String> seeks = new ArrayList<>();
    RuntimeException createFailure;
    boolean failAfterCreate;
    boolean failSeek;
    boolean closed;
    int creates;
    int listCalls;

    @Override
    public String create(String name) {
      creates++;
      if (createFailure != null) throw createFailure;
      String fullName = fullName(name);
      resources.add(fullName);
      if (failAfterCreate) {
        throw new IllegalStateException("Injected ambiguous create failure");
      }
      return fullName;
    }

    @Override
    public void seek(String name) {
      if (failSeek) {
        throw new IllegalStateException("Injected seek RPC failure");
      }
      if (!resources.contains(name)) {
        throw new IllegalStateException("Replay snapshot is missing");
      }
      seeks.add(name);
    }

    @Override
    public void delete(String name) {
      if (failedDeletions.contains(fullName(name))) {
        throw new IllegalStateException("Injected delete failure");
      }
      resources.remove(fullName(name));
    }

    @Override
    public boolean exists(String name) {
      // Production uses GetSnapshot, not a project-wide list, for this lookup.
      return resources.contains(fullName(name));
    }

    @Override
    public long expiryMillis(String name) {
      return expired.contains(name) ? 1 : expiries.getOrDefault(name, Long.MAX_VALUE);
    }

    @Override
    public Iterable<String> list() {
      listCalls++;
      return new ArrayList<>(resources);
    }

    @Override
    public void close() {
      closed = true;
    }

    private String fullName(String name) {
      return name.contains("/") ? name : "projects/project/snapshots/" + name;
    }
  }
}

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

import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
  public void freshRestart_reclaimsUncheckpointedAttemptsWithinStableNamespace() {
    String namespace = "00000000000000000000000000000000";
    PubSubReplaySnapshots firstAttempt = PubSubReplaySnapshots.startFresh(store, 1, namespace);
    snapshot(firstAttempt, 1);
    snapshot(firstAttempt, 2);
    PubSubEnumeratorCheckpoint unrelated = snapshot(new PubSubReplaySnapshots(store, 1), 1);
    PubSubReplaySnapshots.startFresh(store, 1, namespace);
    assertThat(store.resources).containsExactly(name(unrelated));
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
    assertThrows(IllegalStateException.class, () -> manager.notifyCheckpointComplete(2));
    int creates = store.creates;
    assertThrows(IllegalStateException.class, () -> snapshot(manager, 3));
    assertThat(store.creates).isEqualTo(creates);
    store.failedDeletions.clear();
    PubSubEnumeratorCheckpoint third = snapshot(manager, 3);
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
  public void recovery_cleansOrphansBeforeNewCheckpointsAndPreservesOtherNamespaces() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 2);
    PubSubEnumeratorCheckpoint checkpoint = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint orphan = snapshot(original, 2);
    original.notifyCheckpointComplete(2);
    PubSubEnumeratorCheckpoint unrelated = snapshot(new PubSubReplaySnapshots(store, 1), 1);
    PubSubReplaySnapshots restored = PubSubReplaySnapshots.restore(store, 2, checkpoint);
    assertThat(store.resources).doesNotContain(name(orphan));
    assertThat(store.resources).contains(name(checkpoint));
    PubSubEnumeratorCheckpoint next = snapshot(restored, 3);
    restored.notifyCheckpointComplete(3);
    assertThat(store.resources).containsExactly(name(checkpoint), name(next), name(unrelated));
  }

  @Test
  public void recovery_retriesFailedDeletionBeforeCreatingAnythingAfterRestart() {
    PubSubReplaySnapshots original = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(original, 1);
    original.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(original, 2);
    store.failedDeletions.add(name(first));
    assertThrows(IllegalStateException.class, () -> original.notifyCheckpointComplete(2));
    int creates = store.creates;

    assertThrows(
        IllegalStateException.class, () -> PubSubReplaySnapshots.restore(store, 1, second));
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
    assertThat(store.resources).containsExactly(name(checkpoint));
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
    final List<String> seeks = new ArrayList<>();
    boolean failAfterCreate;
    boolean failSeek;
    boolean closed;
    int creates;

    @Override
    public String create(String name) {
      creates++;
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
    public Iterable<String> list() {
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

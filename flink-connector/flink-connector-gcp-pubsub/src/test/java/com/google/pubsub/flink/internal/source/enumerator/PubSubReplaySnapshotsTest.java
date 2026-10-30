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
  public void completion_retainsLatestCompletedAndAllInFlightBoundaries() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 1);
    PubSubEnumeratorCheckpoint first = snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint second = snapshot(manager, 2);
    assertThrows(IllegalStateException.class, () -> snapshot(manager, 3));
    assertThat(store.resources).containsExactly(name(first), name(second));
    manager.notifyCheckpointComplete(2);
    PubSubEnumeratorCheckpoint third = snapshot(manager, 3);
    assertThat(store.resources).containsExactly(name(second), name(third));
    manager.notifyCheckpointComplete(
        1); // Late notification must not regress the protected boundary.
    assertThat(store.resources).containsExactly(name(second), name(third));
    manager.notifyCheckpointComplete(3);
    assertThat(store.resources).containsExactly(name(third));
  }

  @Test
  public void incompleteBoundaries_hardLimitBlocksAdditionalCreates() {
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, 2);
    snapshot(manager, 1);
    snapshot(manager, 2);
    snapshot(manager, 3);
    assertThrows(IllegalStateException.class, () -> snapshot(manager, 4));
    assertThat(store.creates).isEqualTo(3);
    assertThat(store.resources).hasSize(3);
    manager.notifyCheckpointComplete(3);
    snapshot(manager, 4);
    assertThat(store.resources).hasSize(3);
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
    snapshot(manager, 1);
    manager.notifyCheckpointComplete(1);
    PubSubEnumeratorCheckpoint savepoint = snapshot(manager, 2); // No completion notification.
    PubSubEnumeratorCheckpoint next = snapshot(manager, 3);
    manager.notifyCheckpointComplete(3);
    assertThat(store.resources).containsExactly(name(savepoint), name(next));
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

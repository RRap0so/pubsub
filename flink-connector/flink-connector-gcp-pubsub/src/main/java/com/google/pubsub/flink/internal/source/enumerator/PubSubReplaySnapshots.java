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

import com.google.common.base.Preconditions;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Coordinator-owned replay snapshots. All methods run on the enumerator thread. */
public final class PubSubReplaySnapshots implements AutoCloseable {
  public interface Store extends AutoCloseable {
    String create(String name);

    void seek(String name);

    void delete(String name);

    Iterable<String> list();

    @Override
    void close();
  }

  private final Store store;
  private final int retentionCount;
  private final String namespace;
  private final NavigableMap<Long, String> snapshots = new TreeMap<>();
  // Flink checkpoint IDs can share an earlier provider boundary. Keep each pending selection
  // stable across repeated snapshot calls and until every dependent checkpoint is finalized.
  private final NavigableMap<Long, Long> checkpointSnapshots = new TreeMap<>();
  // Keep failed deletions until successful; never silently forget a leaked resource.
  private final Set<String> deletions = new LinkedHashSet<>();
  private long completedCheckpoint = Long.MIN_VALUE;

  public PubSubReplaySnapshots(Store store, int retentionCount) {
    this(store, retentionCount, UUID.randomUUID().toString().replace("-", ""));
  }

  private PubSubReplaySnapshots(Store store, int retentionCount, String namespace) {
    Preconditions.checkArgument(retentionCount > 0, "replay snapshot retention must be positive");
    this.store = store;
    this.retentionCount = retentionCount;
    this.namespace = namespace;
  }

  public static PubSubReplaySnapshots startFresh(
      Store store, int retentionCount, String namespace) {
    Preconditions.checkArgument(
        namespace.matches("[0-9a-f]{32}"), "Invalid replay snapshot namespace");
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, retentionCount, namespace);
    manager.reconcileNamespace();
    return manager;
  }

  public static PubSubReplaySnapshots restore(
      Store store, int retentionCount, PubSubEnumeratorCheckpoint checkpoint) {
    String namespace = checkpoint.getReplaySnapshotNamespace();
    Preconditions.checkArgument(
        namespace.matches("[0-9a-f]{32}"), "Checkpoint has no valid replay snapshot namespace");
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, retentionCount, namespace);
    for (PubSubEnumeratorCheckpoint.ReplaySnapshot snapshot : checkpoint.getReplaySnapshotsList()) {
      Preconditions.checkArgument(
          snapshot.getCheckpointId() <= checkpoint.getReplayCheckpointId(),
          "Checkpoint references a future replay boundary");
      String name = snapshot.getName();
      String shortName = name.substring(name.lastIndexOf('/') + 1);
      Preconditions.checkArgument(
          shortName.matches("flink-" + namespace + "-[0-9a-f]{32}"),
          "Checkpoint references a snapshot outside its namespace");
      manager.snapshots.put(snapshot.getCheckpointId(), snapshot.getName());
    }
    String target = manager.snapshots.get(checkpoint.getReplayCheckpointId());
    Preconditions.checkArgument(target != null, "Checkpoint has no replay snapshot reference");
    // Must finish before any source reader is assigned input. Never fall back on a failed seek.
    store.seek(target);
    manager.completedCheckpoint = checkpoint.getReplayCheckpointId();
    manager.pruneCompletedSnapshots();
    // Reconcile before creating new resources, including after repeated restarts following a
    // deletion failure. The selected restore boundary remains protected throughout cleanup.
    manager.reconcileNamespace();
    return manager;
  }

  private void reconcileNamespace() {
    Set<String> referenced = new HashSet<>(snapshots.values());
    String prefix = "flink-" + namespace + "-";
    for (String name : store.list()) {
      String shortName = name.substring(name.lastIndexOf('/') + 1);
      if (shortName.startsWith(prefix) && !referenced.contains(name)) {
        deletions.add(name);
      }
    }
    deleteQueuedSnapshots();
  }

  public void snapshot(long checkpointId, PubSubEnumeratorCheckpoint.Builder checkpoint) {
    deleteQueuedSnapshots();
    Long replayCheckpointId = checkpointSnapshots.get(checkpointId);
    if (replayCheckpointId == null) {
      replayCheckpointId = checkpointId;
      if (!snapshots.containsKey(checkpointId)) {
        Preconditions.checkState(
            checkpointId > completedCheckpoint, "Cannot recreate a finalized replay boundary");
        if (!snapshots.isEmpty() && snapshots.lastKey() > completedCheckpoint) {
          // A savepoint may complete without a completion notification. Reuse its earlier
          // boundary so the next checkpoint can finish without allocating another snapshot.
          // The snapshot predates both cuts; recovery may replay extra data, never skip input.
          replayCheckpointId = snapshots.lastKey();
          Preconditions.checkArgument(
              replayCheckpointId <= checkpointId, "Cannot use a future replay boundary");
        } else {
          Preconditions.checkState(
              (long) snapshots.size() < (long) retentionCount + 1,
              "Replay snapshot retention invariant violated");
          String name = "flink-" + namespace + "-" + UUID.randomUUID().toString().replace("-", "");
          try {
            name = store.create(name);
          } catch (RuntimeException failure) {
            // A failed create RPC may have succeeded remotely. Retry cleanup before making more.
            deletions.add(name);
            throw failure;
          }
          snapshots.put(checkpointId, name);
        }
      }
      checkpointSnapshots.put(checkpointId, replayCheckpointId);
    }
    checkpoint.setReplaySnapshotNamespace(namespace).setReplayCheckpointId(replayCheckpointId);
    for (Map.Entry<Long, String> entry : snapshots.headMap(replayCheckpointId, true).entrySet()) {
      checkpoint.addReplaySnapshots(
          PubSubEnumeratorCheckpoint.ReplaySnapshot.newBuilder()
              .setCheckpointId(entry.getKey())
              .setName(entry.getValue()));
    }
  }

  public void notifyCheckpointComplete(long checkpointId) {
    completedCheckpoint = Math.max(completedCheckpoint, checkpointId);
    checkpointSnapshots.headMap(completedCheckpoint, true).clear();
    pruneCompletedSnapshots();
    deleteQueuedSnapshots();
  }

  private void pruneCompletedSnapshots() {
    // A checkpoint/savepoint may have no completion callback of its own. Keep the last N
    // provider boundaries at or before the newest completed checkpoint, plus one shared pending
    // one.
    while (snapshots.headMap(completedCheckpoint, true).size() > retentionCount) {
      deletions.add(snapshots.pollFirstEntry().getValue());
    }
  }

  public void notifyCheckpointAborted(long checkpointId) {
    Long replayCheckpointId = checkpointSnapshots.remove(checkpointId);
    if (replayCheckpointId != null
        && replayCheckpointId > completedCheckpoint
        && !checkpointSnapshots.containsValue(replayCheckpointId)) {
      String name = snapshots.remove(replayCheckpointId);
      if (name != null) {
        deletions.add(name);
      }
    }
    deleteQueuedSnapshots();
  }

  private void deleteQueuedSnapshots() {
    for (String name : new ArrayList<>(deletions)) {
      store.delete(name);
      deletions.remove(name);
    }
  }

  @Override
  public void close() {
    // Completed snapshots must outlive the enumerator for future checkpoint/savepoint restores.
    store.close();
  }
}

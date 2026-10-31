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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinator-owned replay snapshots. All methods run on the enumerator thread. */
public final class PubSubReplaySnapshots implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubReplaySnapshots.class);

  public interface Store extends AutoCloseable {
    String create(String name);

    void seek(String name);

    void delete(String name);

    Iterable<String> list();

    default boolean exists(String name) {
      for (String existing : list()) {
        if (existing.equals(name) || existing.endsWith("/" + name)) return true;
      }
      return false;
    }

    default long expiryMillis(String name) {
      return Long.MAX_VALUE;
    }

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
  // Inventory is listed once per coordinator lifetime. Never infer that unknown snapshots are
  // uncommitted: they may belong to newer savepoints. Track our own mutations locally.
  private final Set<String> resources = new HashSet<>();
  // Persist failed deletions in the next checkpoint so cleanup survives recovery.
  private final Set<String> deletions = Collections.synchronizedSet(new LinkedHashSet<>());
  private final ThreadSafeSimpleCounter deleteFailures = new ThreadSafeSimpleCounter();
  private final ThreadSafeSimpleCounter createFailures = new ThreadSafeSimpleCounter();
  private volatile long selectedExpiryMillis;
  private long completedCheckpoint = Long.MIN_VALUE;

  public PubSubReplaySnapshots(Store store, int retentionCount) {
    this(store, retentionCount, UUID.randomUUID().toString().replace("-", ""));
  }

  private PubSubReplaySnapshots(Store store, int retentionCount, String namespace) {
    Preconditions.checkArgument(retentionCount > 0, "replay snapshot retention must be positive");
    this.store = store;
    this.retentionCount = retentionCount;
    this.namespace = namespace;
    String prefix = "flink-" + namespace + "-";
    for (String name : store.list()) {
      if (shortName(name).startsWith(prefix)) resources.add(shortName(name));
    }
  }

  public static PubSubReplaySnapshots startFresh(
      Store store, int retentionCount, String namespace) {
    Preconditions.checkArgument(
        namespace.matches("[0-9a-f]{32}"), "Invalid replay snapshot namespace");
    PubSubReplaySnapshots manager = new PubSubReplaySnapshots(store, retentionCount, namespace);
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
      manager.resources.add(shortName);
    }
    String target = manager.snapshots.get(checkpoint.getReplayCheckpointId());
    Preconditions.checkArgument(target != null, "Checkpoint has no replay snapshot reference");
    Set<String> retained = new HashSet<>();
    for (String name : manager.snapshots.values()) retained.add(shortName(name));
    String projectPrefix = target.substring(0, target.lastIndexOf('/') + 1);
    for (String name : checkpoint.getPendingSnapshotDeletionsList()) {
      Preconditions.checkArgument(
          shortName(name).matches("flink-" + namespace + "-[0-9a-f]{32}")
              && (!name.contains("/") || name.equals(projectPrefix + shortName(name)))
              && !retained.contains(shortName(name)),
          "Checkpoint contains an unsafe pending snapshot deletion");
      manager.deletions.add(name);
      manager.resources.add(shortName(name));
    }
    // Must finish before any source reader is assigned input. Never fall back on a failed seek.
    store.seek(target);
    manager.completedCheckpoint = checkpoint.getReplayCheckpointId();
    manager.pruneCompletedSnapshots();
    // Unreferenced snapshots may belong to completed newer savepoints. Never infer that they
    // are uncommitted from an older checkpoint; leave them for provider expiry.
    manager.deleteQueuedSnapshots();
    manager.selectedExpiryMillis = store.expiryMillis(target);
    Preconditions.checkState(
        manager.selectedExpiryMillis > System.currentTimeMillis(),
        "Selected restore boundary is missing or expired");
    return manager;
  }

  public void registerMetrics(MetricGroup group) {
    group.counter("snapshotDeleteFailures", deleteFailures);
    group.counter("snapshotCreateFailures", createFailures);
    group.gauge("pendingSnapshotDeletions", () -> deletions.size());
    group.gauge(
        "replaySnapshotTimeToExpiryMillis",
        () ->
            selectedExpiryMillis == 0
                ? 0L
                : Math.max(0L, selectedExpiryMillis - System.currentTimeMillis()));
  }

  private static String shortName(String name) {
    return name.substring(name.lastIndexOf('/') + 1);
  }

  private boolean withinResourceBudget() {
    return (long) resources.size() < (long) retentionCount + 1;
  }

  private long reusableBoundary(long checkpointId) {
    Map.Entry<Long, String> boundary = snapshots.floorEntry(checkpointId);
    Preconditions.checkState(boundary != null, "No valid replay boundary is available");
    Preconditions.checkState(
        store.exists(boundary.getValue())
            && store.expiryMillis(boundary.getValue()) > System.currentTimeMillis(),
        "The replay boundary has expired; cannot safely complete a checkpoint");
    return boundary.getKey();
  }

  public void snapshot(long checkpointId, PubSubEnumeratorCheckpoint.Builder checkpoint) {
    deleteQueuedSnapshots();
    for (var entry : new ArrayList<>(snapshots.entrySet())) {
      if (store.expiryMillis(entry.getValue()) <= System.currentTimeMillis()) {
        snapshots.remove(entry.getKey());
        deletions.add(entry.getValue());
      }
    }
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
          if (!withinResourceBudget()) {
            replayCheckpointId = reusableBoundary(checkpointId);
          } else {
            String name =
                "flink-" + namespace + "-" + UUID.randomUUID().toString().replace("-", "");
            try {
              name = store.create(name);
            } catch (RuntimeException failure) {
              // A failed create RPC may have succeeded remotely. Retry cleanup before making more.
              deletions.add(name);
              resources.add(shortName(name));
              createFailures.inc();
              LOG.warn(
                  "Could not create replay snapshot; reusing an earlier valid boundary if"
                      + " available",
                  failure);
              if (snapshots.isEmpty()) throw failure;
              replayCheckpointId = reusableBoundary(checkpointId);
              name = null;
            }
            if (name != null) {
              snapshots.put(checkpointId, name);
              resources.add(shortName(name));
            }
          }
        }
      }
      checkpointSnapshots.put(checkpointId, replayCheckpointId);
    }
    Preconditions.checkState(
        snapshots.containsKey(replayCheckpointId), "Selected replay boundary expired");
    selectedExpiryMillis = store.expiryMillis(snapshots.get(replayCheckpointId));
    checkpoint.setReplaySnapshotNamespace(namespace).setReplayCheckpointId(replayCheckpointId);
    checkpoint.addAllPendingSnapshotDeletions(new ArrayList<>(deletions));
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
      try {
        store.delete(name);
        deletions.remove(name);
        resources.remove(shortName(name));
      } catch (RuntimeException failure) {
        deleteFailures.inc();
        LOG.warn("Replay snapshot deletion will be retried: {}", name, failure);
      }
    }
  }

  @Override
  public void close() {
    // Completed snapshots must outlive the enumerator for future checkpoint/savepoint restores.
    store.close();
  }
}

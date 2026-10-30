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
package com.google.pubsub.flink;

import com.google.common.base.Preconditions;
import com.google.pubsub.flink.internal.source.enumerator.PubSubSnapshotStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Explicit offline cleanup of replay snapshots no longer needed by any retained recovery point. */
public final class PubSubReplaySnapshotCleanup {
  private PubSubReplaySnapshotCleanup() {}

  /**
   * Lists an exact namespace without deleting anything. Obtain the namespace from the job's
   * persisted enumerator checkpoint. Names must be reviewed before calling {@link #deleteReviewed}.
   *
   * <p>Stop the job before cleanup. Delete only snapshots not needed by any checkpoint or savepoint
   * you intend to restore. Restart the job afterward to refresh its namespace inventory. Snapshot
   * metadata identifies the topic, not its originating subscription; the exact namespace is the
   * ownership token. The configured subscription must still exist so its topic can be checked. This
   * utility cannot detect active jobs or coordinate with their operators.
   */
  public static List<String> list(PubSubSource<?> source, String namespace) throws IOException {
    validateNamespace(namespace);
    try (PubSubSnapshotStore store = open(source)) {
      return list(store, source.projectName(), namespace);
    }
  }

  /**
   * Deletes only the explicitly supplied, previously reviewed snapshot names. Other namespaces and
   * snapshots created after review remain untouched. Every name must match the exact project and
   * namespace; the subscription topic is checked before any deletion. Missing resources are
   * harmless, and the same list can be retried after partial RPC failure.
   *
   * <p>This permanently removes recovery boundaries. Invoke only while the job is stopped and only
   * for snapshots no retained checkpoint or savepoint needs. Never schedule automatic cleanup based
   * solely on age.
   */
  public static void deleteReviewed(
      PubSubSource<?> source, String namespace, List<String> reviewedSnapshots) throws IOException {
    List<String> names = validateReviewed(source.projectName(), namespace, reviewedSnapshots);
    if (names.isEmpty()) {
      return;
    }
    try (PubSubSnapshotStore store = open(source)) {
      store.deleteForSubscriptionTopic(names);
    }
  }

  static List<String> list(PubSubSnapshotStore store, String project, String namespace) {
    validateNamespace(namespace);
    List<String> names = new ArrayList<>();
    for (String name : store.listForSubscriptionTopic()) {
      if (matches(project, namespace, name)) {
        names.add(name);
      }
    }
    Collections.sort(names);
    return Collections.unmodifiableList(names);
  }

  static List<String> validateReviewed(
      String project, String namespace, List<String> reviewedSnapshots) {
    validateNamespace(namespace);
    Preconditions.checkNotNull(reviewedSnapshots, "Reviewed snapshot names are required");
    Set<String> names = new LinkedHashSet<>();
    for (String name : reviewedSnapshots) {
      Preconditions.checkArgument(
          matches(project, namespace, name),
          "Reviewed snapshot is outside the exact project and namespace");
      names.add(name);
    }
    return new ArrayList<>(names);
  }

  private static boolean matches(String project, String namespace, String name) {
    if (name == null) {
      return false;
    }
    String prefix = "projects/" + project + "/snapshots/flink-" + namespace + "-";
    return name.startsWith(prefix) && name.substring(prefix.length()).matches("[0-9a-f]{32}");
  }

  private static void validateNamespace(String namespace) {
    Preconditions.checkArgument(
        namespace != null && namespace.matches("[0-9a-f]{32}"),
        "An exact replay snapshot namespace is required");
  }

  private static PubSubSnapshotStore open(PubSubSource<?> source) throws IOException {
    return new PubSubSnapshotStore(
        source.projectName(), source.subscriptionName(), source.credentials(), source.endpoint());
  }
}

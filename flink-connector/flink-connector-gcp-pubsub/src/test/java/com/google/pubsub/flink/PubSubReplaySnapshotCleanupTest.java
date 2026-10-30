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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.pubsub.flink.internal.source.enumerator.PubSubSnapshotStore;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PubSubReplaySnapshotCleanupTest {
  private static final String NAMESPACE = "00000000000000000000000000000000";
  private static final String PREFIX = "projects/project/snapshots/flink-" + NAMESPACE + "-";
  private static final String FIRST = PREFIX + "11111111111111111111111111111111";
  private static final String SECOND = PREFIX + "22222222222222222222222222222222";

  @Test
  public void listingRequiresExactNamespaceProjectAndResourceShape() {
    PubSubSnapshotStore store = mock(PubSubSnapshotStore.class);
    when(store.listForSubscriptionTopic())
        .thenReturn(
            List.of(
                SECOND,
                FIRST,
                "projects/another/snapshots/flink-"
                    + NAMESPACE
                    + "-11111111111111111111111111111111",
                "projects/project/snapshots/flink-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-11111111111111111111111111111111",
                PREFIX + "arbitrary",
                FIRST + "-additional"));
    List<String> names = PubSubReplaySnapshotCleanup.list(store, "project", NAMESPACE);
    assertThat(names).containsExactly(FIRST, SECOND).inOrder();
    assertThrows(UnsupportedOperationException.class, () -> names.add("other"));
  }

  @Test
  public void listingRejectsEmptyPartialAndWildcardNamespacesBeforeRpc() {
    PubSubSnapshotStore store = mock(PubSubSnapshotStore.class);
    for (String namespace : List.of("", "*", "0000", NAMESPACE + "0")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> PubSubReplaySnapshotCleanup.list(store, "project", namespace));
    }
    verifyNoInteractions(store);
  }

  @Test
  public void reviewedListRejectsForeignProjectNamespaceAndUnqualifiedNames() {
    for (String name :
        List.of(
            "flink-" + NAMESPACE + "-11111111111111111111111111111111",
            FIRST.replace("projects/project/", "projects/another/"),
            FIRST.replace(NAMESPACE, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            FIRST + "-extra")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PubSubReplaySnapshotCleanup.validateReviewed(
                  "project", NAMESPACE, List.of(FIRST, name)));
    }
  }

  @Test
  public void reviewedListIsCopiedDeduplicatedAndDoesNotDiscoverNewSnapshots() {
    List<String> original = new java.util.ArrayList<>(List.of(FIRST, FIRST));
    List<String> reviewed =
        PubSubReplaySnapshotCleanup.validateReviewed("project", NAMESPACE, original);
    original.add(SECOND);
    assertThat(reviewed).containsExactly(FIRST);
  }
}

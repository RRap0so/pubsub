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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSnapshotName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Snapshot;
import com.google.pubsub.v1.Subscription;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PubSubSnapshotStoreTest {
  private static final String TOPIC = "projects/project/topics/topic";
  private static final ProjectSnapshotName FIRST = ProjectSnapshotName.of("project", "first");
  private static final ProjectSnapshotName SECOND = ProjectSnapshotName.of("project", "second");
  private SubscriptionAdminClient client;
  private PubSubSnapshotStore store;

  @Before
  public void setUp() {
    client = mock(SubscriptionAdminClient.class);
    store = new PubSubSnapshotStore("project", "subscription", client);
  }

  @Test
  public void listingExcludesSnapshotsOnOtherTopics() {
    stubSubscription();
    var response = mock(SubscriptionAdminClient.ListSnapshotsPagedResponse.class);
    when(client.listSnapshots("projects/project")).thenReturn(response);
    when(response.iterateAll())
        .thenReturn(
            List.of(
                Snapshot.newBuilder().setName(FIRST.toString()).setTopic(TOPIC).build(),
                Snapshot.newBuilder()
                    .setName(SECOND.toString())
                    .setTopic("projects/project/topics/other")
                    .build()));
    assertThat(store.listForSubscriptionTopic()).containsExactly(FIRST.toString());
    verify(client, never()).deleteSnapshot(any(ProjectSnapshotName.class));
  }

  @Test
  public void entireListIsValidatedBeforeAnyDeletion() {
    stubSubscription();
    when(client.getSnapshot(FIRST)).thenReturn(snapshot(FIRST, TOPIC));
    when(client.getSnapshot(SECOND)).thenReturn(snapshot(SECOND, "projects/project/topics/other"));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.deleteForSubscriptionTopic(List.of(FIRST.toString(), SECOND.toString())));
    verify(client, never()).deleteSnapshot(any(ProjectSnapshotName.class));
  }

  @Test
  public void foreignProjectsFailBeforeAnyRpc() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store.deleteForSubscriptionTopic(
                List.of(FIRST.toString(), "projects/other/snapshots/second")));
    verifyNoInteractions(client);
  }

  @Test
  public void missingSubscriptionFailsClosed() {
    when(client.getSubscription(ProjectSubscriptionName.of("project", "subscription")))
        .thenThrow(mock(NotFoundException.class));
    assertThrows(
        NotFoundException.class, () -> store.deleteForSubscriptionTopic(List.of(FIRST.toString())));
    verify(client, never()).deleteSnapshot(any(ProjectSnapshotName.class));
  }

  @Test
  public void expiredSnapshotsAndRepeatedCleanupAreHarmless() {
    stubSubscription();
    when(client.getSnapshot(FIRST)).thenThrow(mock(NotFoundException.class));
    doThrow(mock(NotFoundException.class)).when(client).deleteSnapshot(FIRST);
    store.deleteForSubscriptionTopic(List.of(FIRST.toString()));
    store.deleteForSubscriptionTopic(List.of(FIRST.toString()));
    verify(client, times(2)).deleteSnapshot(FIRST);
  }

  @Test
  public void rpcFailureCanBeRetriedWithSameReviewedList() {
    stubSubscription();
    when(client.getSnapshot(FIRST)).thenReturn(snapshot(FIRST, TOPIC));
    when(client.getSnapshot(SECOND)).thenReturn(snapshot(SECOND, TOPIC));
    doThrow(new IllegalStateException("temporary failure"))
        .doNothing()
        .when(client)
        .deleteSnapshot(SECOND);
    List<String> reviewed = List.of(FIRST.toString(), SECOND.toString());
    assertThrows(IllegalStateException.class, () -> store.deleteForSubscriptionTopic(reviewed));
    store.deleteForSubscriptionTopic(reviewed);
    verify(client, times(2)).deleteSnapshot(FIRST);
    verify(client, times(2)).deleteSnapshot(SECOND);
  }

  private void stubSubscription() {
    when(client.getSubscription(ProjectSubscriptionName.of("project", "subscription")))
        .thenReturn(Subscription.newBuilder().setTopic(TOPIC).build());
  }

  private static Snapshot snapshot(ProjectSnapshotName name, String topic) {
    return Snapshot.newBuilder().setName(name.toString()).setTopic(topic).build();
  }
}

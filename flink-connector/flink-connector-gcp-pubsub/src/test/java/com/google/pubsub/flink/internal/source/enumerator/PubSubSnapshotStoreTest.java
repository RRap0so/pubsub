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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSnapshotName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PubSubSnapshotStoreTest {
  private static final ProjectSnapshotName FIRST = ProjectSnapshotName.of("project", "first");
  private SubscriptionAdminClient client;
  private PubSubSnapshotStore store;

  @Before
  public void setUp() {
    client = mock(SubscriptionAdminClient.class);
    store = new PubSubSnapshotStore("project", "subscription", client);
  }

  @Test
  public void missingHistoricalSnapshot_hasNoRemainingLifetime() {
    when(client.getSnapshot(FIRST)).thenThrow(mock(NotFoundException.class));
    assertThat(store.expiryMillis(FIRST.toString())).isEqualTo(0L);
    assertThat(store.exists(FIRST.toString())).isFalse();
  }

  @Test
  public void expiredSnapshotDeletion_isIdempotent() {
    doThrow(mock(NotFoundException.class)).when(client).deleteSnapshot(FIRST);
    store.delete(FIRST.toString());
    store.delete(FIRST.toString());
    verify(client, times(2)).deleteSnapshot(FIRST);
  }
}

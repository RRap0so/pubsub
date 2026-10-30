/*
 * Copyright 2023 Google LLC
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
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class PubSubCheckpointSerializerTest {

  @Test
  public void testSerialization() throws Exception {
    List<PubSubEnumeratorCheckpoint.Assignment> assignments = new ArrayList<>();
    assignments.add(PubSubEnumeratorCheckpoint.Assignment.newBuilder().setSubtask(1).build());
    PubSubEnumeratorCheckpoint proto =
        PubSubEnumeratorCheckpoint.newBuilder().addAllAssignments(assignments).build();
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(proto)))
        .isEqualTo(proto);
  }

  @Test
  public void deserialize_readsLegacyVersionZero() throws Exception {
    // Golden bytes generated with master's serializer and proto at
    // cd7d07bbfa4b81afc148f31a091f9b0d958dbe01.
    byte[] bytes;
    try (var input = getClass().getResourceAsStream("/checkpoints/enumerator-v0.bin")) {
      assertThat(input).isNotNull();
      bytes = input.readAllBytes();
    }
    var checkpoint = new PubSubCheckpointSerializer().deserialize(0, bytes);
    assertThat(checkpoint.getAssignmentsCount()).isEqualTo(1);
    assertThat(checkpoint.getAssignments(0).getSubtask()).isEqualTo(3);
    assertThat(checkpoint.getAssignments(0).getSplit().getSubscription())
        .isEqualTo("projects/golden/subscriptions/input");
    assertThat(checkpoint.getAssignments(0).getSplit().getUid()).isEqualTo("reader-3");
    assertThat(checkpoint.getReplaySnapshotNamespace()).isEmpty();
    assertThat(checkpoint.getReplayDeduplicationRequired()).isFalse();
  }

  @Test
  public void deserialize_readsVersionOneAndPreservesDeduplicationModeInVersionTwo()
      throws Exception {
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    assertThat(
            serializer
                .deserialize(1, PubSubEnumeratorCheckpoint.getDefaultInstance().toByteArray())
                .getReplayDeduplicationRequired())
        .isFalse();
    PubSubEnumeratorCheckpoint protectedCheckpoint =
        PubSubEnumeratorCheckpoint.newBuilder().setReplayDeduplicationRequired(true).build();
    assertThat(serializer.deserialize(2, serializer.serialize(protectedCheckpoint)))
        .isEqualTo(protectedCheckpoint);
  }

  @Test
  public void serialization_preservesPendingDeletionsInVersionThree() throws Exception {
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    PubSubEnumeratorCheckpoint checkpoint =
        PubSubEnumeratorCheckpoint.newBuilder()
            .addPendingSnapshotDeletions("flink-namespace-candidate")
            .build();
    assertThat(serializer.getVersion()).isEqualTo(3);
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint)))
        .isEqualTo(checkpoint);
    assertThat(
            serializer
                .deserialize(2, PubSubEnumeratorCheckpoint.getDefaultInstance().toByteArray())
                .getPendingSnapshotDeletionsList())
        .isEmpty();
  }

  @Test
  public void deserialize_rejectsUnknownVersion() {
    assertThrows(
        java.io.IOException.class,
        () -> new PubSubCheckpointSerializer().deserialize(4, new byte[0]));
  }
}

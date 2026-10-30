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
package com.google.pubsub.flink;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceTest {
  @Test
  public void build_invalidSubscription() throws Exception {
    assertThrows(
        NullPointerException.class, () -> PubSubSource.<String>builder().setSubscriptionName(null));
    PubSubSource.Builder<String> builder =
        PubSubSource.<String>builder()
            .setProjectName("project")
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()));
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void build_invalidProject() throws Exception {
    assertThrows(
        NullPointerException.class, () -> PubSubSource.<String>builder().setProjectName(null));
    PubSubSource.Builder<String> builder =
        PubSubSource.<String>builder()
            .setSubscriptionName("subscription")
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()));
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void build_invalidSchema() throws Exception {
    assertThrows(
        NullPointerException.class,
        () -> PubSubSource.<String>builder().setDeserializationSchema(null));
    PubSubSource.Builder<String> builder =
        PubSubSource.<String>builder().setProjectName("project").setSubscriptionName("sub");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void build_nullMaxOutstandingMessagesCountThrows() throws Exception {
    assertThrows(
        NullPointerException.class,
        () -> PubSubSource.<String>builder().setMaxOutstandingMessagesCount(null));
  }

  @Test
  public void build_negativeMaxOutstandingMessagesCountThrows() throws Exception {
    PubSubSource.Builder<String> builder =
        PubSubSource.<String>builder()
            .setProjectName("project")
            .setSubscriptionName("subscription")
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
            .setMaxOutstandingMessagesCount(-1L);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void build_nullMaxOutstandingMessagesBytesThrows() throws Exception {
    assertThrows(
        NullPointerException.class,
        () -> PubSubSource.<String>builder().setMaxOutstandingMessagesBytes(null));
  }

  @Test
  public void build_negativeMaxOutstandingMessagesBytesThrows() throws Exception {
    PubSubSource.Builder<String> builder =
        PubSubSource.<String>builder()
            .setProjectName("project")
            .setSubscriptionName("subscription")
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
            .setMaxOutstandingMessagesBytes(-1L);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void build_nullParallelPullCountThrows() throws Exception {
    assertThrows(
        NullPointerException.class,
        () -> PubSubSource.<String>builder().setParallelPullCount(null));
  }

  @Test
  public void build_negativeParallelPullCountThrows() throws Exception {
    PubSubSource.Builder<String> builder =
        PubSubSource.<String>builder()
            .setProjectName("project")
            .setSubscriptionName("subscription")
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
            .setParallelPullCount(-1);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void build_invalidCreds() throws Exception {
    assertThrows(
        NullPointerException.class, () -> PubSubSource.<String>builder().setCredentials(null));
  }

  @Test
  public void build_invalidEndpoint() throws Exception {
    assertThrows(
        NullPointerException.class, () -> PubSubSource.<String>builder().setEndpoint(null));
  }

  @Test
  public void build_replaySnapshotsDisabledByDefault() {
    assertThat(replayBuilder().build().replaySnapshotRetentionCount().isPresent()).isFalse();
  }

  @Test
  public void build_replaySnapshotRetentionMustBePositive() {
    assertThrows(
        IllegalArgumentException.class,
        () -> replayBuilder().setReplaySnapshotRetentionCount(0).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> replayBuilder().setReplaySnapshotRetentionCount(-1).build());
  }

  @Test
  public void restore_legacyCheckpointCannotEnableReplayRetroactively() {
    PubSubSource<String> source = replayBuilder().setReplaySnapshotRetentionCount(2).build();
    assertThrows(
        IllegalStateException.class,
        () ->
            source.restoreEnumerator(
                null,
                com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint.getDefaultInstance()));
  }

  @Test
  public void restore_replayCheckpointRequiresReplayConfiguration() {
    PubSubSource<String> source = replayBuilder().build();
    var checkpoint =
        com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint.newBuilder()
            .setReplaySnapshotNamespace("00000000000000000000000000000000")
            .build();
    assertThrows(IllegalStateException.class, () -> source.restoreEnumerator(null, checkpoint));
  }

  @Test
  public void restore_replayCheckpointMustMatchConfiguredSubscription() {
    PubSubSource<String> source = replayBuilder().setReplaySnapshotRetentionCount(2).build();
    var checkpoint =
        com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint.newBuilder()
            .setReplaySnapshotNamespace("00000000000000000000000000000000")
            .setReplaySubscription("projects/project/subscriptions/another-subscription")
            .build();
    assertThrows(IllegalArgumentException.class, () -> source.restoreEnumerator(null, checkpoint));
  }

  @Test
  public void restore_legacyCheckpointWorksWithReplayDisabled() throws Exception {
    PubSubSource<String> source = replayBuilder().build();
    var checkpoint =
        com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint.newBuilder()
            .addAssignments(
                com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint.Assignment.newBuilder()
                    .setSubtask(0)
                    .setSplit(
                        com.google.pubsub.flink.proto.SubscriptionSplitProto.newBuilder()
                            .setSubscription("projects/project/subscriptions/subscription")
                            .setUid("0")))
            .build();
    assertThat(source.restoreEnumerator(null, checkpoint).snapshotState(2)).isEqualTo(checkpoint);
  }

  private PubSubSource.Builder<String> replayBuilder() {
    return PubSubSource.<String>builder()
        .setProjectName("project")
        .setSubscriptionName("subscription")
        .setDeserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()));
  }
}

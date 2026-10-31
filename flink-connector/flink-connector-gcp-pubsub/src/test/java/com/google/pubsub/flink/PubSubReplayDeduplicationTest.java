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

import com.google.protobuf.ByteString;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import com.google.pubsub.v1.PubsubMessage;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PubSubReplayDeduplicationTest {
  private static final Duration RETENTION = Duration.ofDays(8);

  @Test
  public void duplicatesAreSuppressedButSeparatePublicationsOfSamePayloadAreKept()
      throws Exception {
    try (var harness = harness(1, 0)) {
      harness.open();
      var first = message("id-1");
      var second = message("id-2");
      harness.processElement(new StreamRecord<>(first, 123));
      harness.processElement(new StreamRecord<>(first, 456));
      harness.processElement(new StreamRecord<>(second, 789));
      assertThat(
              harness.extractOutputValues().stream()
                  .map(value -> value.f0)
                  .collect(java.util.stream.Collectors.toList()))
          .containsExactly("id-1", "id-2")
          .inOrder();
      assertThat(harness.extractOutputStreamRecords().get(0).getTimestamp()).isEqualTo(123);
    }
  }

  @Test
  public void checkpointRestoresSeenIdsWithoutSuppressingNewInput() throws Exception {
    OperatorSubtaskState checkpoint;
    try (var original = harness(1, 0)) {
      original.open();
      original.processElement(new StreamRecord<>(message("id-1")));
      checkpoint = original.snapshot(1, 1);
    }
    try (var restored = harness(1, 0)) {
      restored.initializeState(checkpoint);
      restored.open();
      restored.processElement(new StreamRecord<>(message("id-1")));
      restored.processElement(new StreamRecord<>(message("id-2")));
      assertThat(
              restored.extractOutputValues().stream()
                  .map(value -> value.f0)
                  .collect(java.util.stream.Collectors.toList()))
          .containsExactly("id-2");
    }
  }

  @Test
  public void rescalingRedistributesSeenIdsToTheirNewOwners() throws Exception {
    OperatorSubtaskState checkpoint;
    try (var original = harness(1, 0)) {
      original.open();
      for (int id = 0; id < 40; id++) {
        original.processElement(new StreamRecord<>(message("id-" + id)));
      }
      checkpoint = original.snapshot(1, 1);
    }
    for (int task = 0; task < 2; task++) {
      try (var restored = harness(2, task)) {
        restored.initializeState(
            AbstractStreamOperatorTestHarness.repartitionOperatorState(checkpoint, 16, 1, 2, task));
        restored.open();
        for (int id = 0; id < 40; id++) {
          String key = "id-" + id;
          if (KeyGroupRangeAssignment.assignKeyToParallelOperator(key, 16, 2) == task) {
            restored.processElement(new StreamRecord<>(message(key)));
          }
        }
        assertThat(restored.extractOutputValues()).isEmpty();
      }
    }
  }

  @Test
  public void duplicateDoesNotExtendRetentionAndExpiredIdCanBeAcceptedAgain() throws Exception {
    try (var harness = harness(1, 0)) {
      harness.open();
      harness.setStateTtlProcessingTime(0);
      harness.processElement(new StreamRecord<>(message("id-1")));
      harness.setStateTtlProcessingTime(RETENTION.toMillis() - 1);
      harness.processElement(new StreamRecord<>(message("id-1")));
      assertThat(harness.extractOutputValues()).hasSize(1);
      harness.setStateTtlProcessingTime(RETENTION.toMillis());
      harness.processElement(new StreamRecord<>(message("id-1")));
      assertThat(harness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  public void sourceSettingsAndFullEnvelopeSurviveWrapping() throws Exception {
    PubSubSource<String> source =
        source()
            .setMaxOutstandingMessagesCount(7L)
            .setMaxOutstandingMessagesBytes(1234L)
            .setParallelPullCount(2)
            .setEndpoint("emulator:///127.0.0.1:1234")
            .build();
    var raw = PubSubReplayDeduplication.rawSource(source);
    assertThat(raw.maxOutstandingMessagesCount()).isEqualTo(source.maxOutstandingMessagesCount());
    assertThat(raw.maxOutstandingMessagesBytes()).isEqualTo(source.maxOutstandingMessagesBytes());
    assertThat(raw.parallelPullCount()).isEqualTo(source.parallelPullCount());
    assertThat(raw.endpoint()).isEqualTo(source.endpoint());
    assertThat(raw.replaySnapshotNamespace()).isEqualTo(source.replaySnapshotNamespace());
    assertThat(raw.replayDeduplicationRequired()).isTrue();
    PubsubMessage envelope =
        PubsubMessage.newBuilder()
            .setMessageId("id-1")
            .setData(ByteString.copyFromUtf8("same payload"))
            .putAttributes("key", "value")
            .build();
    assertThat(PubsubMessage.parseFrom(raw.deserializationSchema().deserialize(envelope).f1))
        .isEqualTo(envelope);
    assertThrows(
        IllegalArgumentException.class,
        () -> raw.deserializationSchema().deserialize(PubsubMessage.getDefaultInstance()));
  }

  @Test
  public void changingDeduplicationModeOnRestoreFailsBeforeSeeking() {
    var plain = source().build();
    var protectedSource = PubSubReplayDeduplication.rawSource(plain);
    assertThrows(
        IllegalStateException.class,
        () ->
            protectedSource.restoreEnumerator(
                null, PubSubEnumeratorCheckpoint.getDefaultInstance()));
    assertThrows(
        IllegalStateException.class,
        () ->
            plain.restoreEnumerator(
                null,
                PubSubEnumeratorCheckpoint.newBuilder()
                    .setReplayDeduplicationRequired(true)
                    .build()));
  }

  @Test
  public void unsafeConfigurationsAreRejected() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    assertThrows(IllegalArgumentException.class, () -> read(env, source().build(), RETENTION));
    env.enableCheckpointing(1000);
    env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
    assertThrows(IllegalArgumentException.class, () -> read(env, source().build(), RETENTION));
    env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
    assertThrows(
        IllegalArgumentException.class, () -> read(env, source().build(), Duration.ofDays(7)));
    var unprotected =
        PubSubSource.<String>builder()
            .setProjectName("project")
            .setSubscriptionName("subscription")
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
            .build();
    assertThrows(IllegalArgumentException.class, () -> read(env, unprotected, RETENTION));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubReplayDeduplication.read(
                env, source().build(), WatermarkStrategy.noWatermarks(), "", RETENTION));
    read(env, source().build(), RETENTION);
  }

  private static void read(
      StreamExecutionEnvironment env, PubSubSource<String> source, Duration ttl) {
    PubSubReplayDeduplication.read(env, source, WatermarkStrategy.noWatermarks(), "pubsub", ttl);
  }

  private static PubSubSource.Builder<String> source() {
    return PubSubSource.<String>builder()
        .setProjectName("project")
        .setSubscriptionName("subscription")
        .setDeserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
        .setReplaySnapshotRetentionCount(2);
  }

  private static Tuple2<String, byte[]> message(String id) {
    return Tuple2.of(id, new byte[] {1});
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          String, Tuple2<String, byte[]>, Tuple2<String, byte[]>>
      harness(int parallelism, int task) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new PubSubReplayDeduplication.Deduplicate(RETENTION)),
        value -> value.f0,
        Types.STRING,
        16,
        parallelism,
        task);
  }
}

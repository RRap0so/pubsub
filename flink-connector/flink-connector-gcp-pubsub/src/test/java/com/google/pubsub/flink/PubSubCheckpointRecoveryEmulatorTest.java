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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Real source reader and ack tracker against a local Pub/Sub emulator. Checkpoint callbacks are
 * driven explicitly: this is not a Flink MiniCluster/HA test.
 */
public class PubSubCheckpointRecoveryEmulatorTest {
  private final HttpClient http = HttpClient.newHttpClient();
  private final List<SourceReader<String, SubscriptionSplit>> readers = new ArrayList<>();
  private final List<io.grpc.ManagedChannel> channels = new ArrayList<>();
  private String endpoint;
  private String topic;
  private String subscription;
  private PubSubSource<String> source;

  @Before
  public void setUp() throws Exception {
    endpoint = System.getenv("CHECKPOINT_TEST_EMULATOR");
    Assume.assumeNotNull(endpoint);
    // Reject non-loopback targets: these tests must never create cloud resources.
    assertTrue(endpoint.matches("127\\.0\\.0\\.1:[0-9]+"));
    String clientEndpoint = System.getenv("PUBSUB_EMULATOR_HOST");
    assertTrue(
        "PUBSUB_EMULATOR_HOST must match the test emulator",
        clientEndpoint == null || clientEndpoint.equals(endpoint));
    String suffix = UUID.randomUUID().toString();
    topic = "projects/checkpoint-repro/topics/topic-" + suffix;
    subscription = "projects/checkpoint-repro/subscriptions/sub-" + suffix;
    api("PUT", topic, "{}");
    api("PUT", subscription, "{\"topic\":\"" + topic + "\",\"ackDeadlineSeconds\":10}");
    source =
        PubSubSource.<String>builder()
            .setProjectName("checkpoint-repro")
            .setSubscriptionName("sub-" + suffix)
            .setEndpoint("emulator:///" + endpoint)
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
            .build();
  }

  @Test(timeout = 30000)
  public void latestCheckpoint_unacknowledgedLaterWorkIsRedelivered() throws Exception {
    SourceReader<String, SubscriptionSplit> original = reader(null);
    TestingReaderOutput<String> before = new TestingReaderOutput<>();
    publish("A");
    awaitCount(original, before, 1);
    List<SubscriptionSplit> checkpoint1 = roundTrip(original.snapshotState(1));
    original.notifyCheckpointComplete(1);
    publish("B");
    awaitCount(original, before, 2);
    original.snapshotState(2); // Failure before checkpoint 2 completes: B must remain unacked.
    original.close();
    readers.remove(original);

    SourceReader<String, SubscriptionSplit> restored = reader(checkpoint1);
    TestingReaderOutput<String> after = new TestingReaderOutput<>();
    awaitCount(restored, after, 1);
    assertTrue(after.getEmittedRecords().contains("B"));
  }

  @Test(timeout = 30000)
  public void oldCheckpoint_restoreDoesNotReplayWorkAcknowledgedByNewerCheckpoint()
      throws Exception {
    SourceReader<String, SubscriptionSplit> original = reader(null);
    TestingReaderOutput<String> before = new TestingReaderOutput<>();
    publish("A");
    awaitCount(original, before, 1);
    List<SubscriptionSplit> checkpoint1 = roundTrip(original.snapshotState(1));
    original.notifyCheckpointComplete(1);
    publish("B");
    awaitCount(original, before, 2);
    original.snapshotState(2);
    original.notifyCheckpointComplete(2);
    original.close(); // Subscriber termination flushes batched ack requests.
    readers.remove(original);

    SourceReader<String, SubscriptionSplit> restored = reader(checkpoint1);
    TestingReaderOutput<String> after = new TestingReaderOutput<>();
    publish("C"); // Positive control: restored subscription really is receiving data.
    awaitCount(restored, after, 1);
    long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (System.nanoTime() < deadline) {
      restored.pollNext(after);
      Thread.sleep(10);
    }
    assertEquals(List.of("C"), after.getEmittedRecords()); // B is absent after rollback.
  }

  @Test(timeout = 30000)
  public void completedCheckpoint_lostCompletionNotificationReplaysAlreadyCheckpointedWork()
      throws Exception {
    SourceReader<String, SubscriptionSplit> original = reader(null);
    TestingReaderOutput<String> before = new TestingReaderOutput<>();
    publish("A");
    awaitCount(original, before, 1);
    List<SubscriptionSplit> checkpoint1 = roundTrip(original.snapshotState(1));
    // Coordinator persisted C1, but reader fails before receiving notifyCheckpointComplete.
    original.close();
    readers.remove(original);

    SourceReader<String, SubscriptionSplit> restored = reader(checkpoint1);
    TestingReaderOutput<String> after = new TestingReaderOutput<>();
    awaitCount(restored, after, 1);
    assertEquals(List.of("A"), after.getEmittedRecords());
  }

  @Test(timeout = 30000)
  public void outstandingLimit_stallsInputUntilSuccessfulCheckpoint() throws Exception {
    source =
        PubSubSource.<String>builder()
            .setProjectName("checkpoint-repro")
            .setSubscriptionName(subscription.substring(subscription.lastIndexOf('/') + 1))
            .setEndpoint("emulator:///" + endpoint)
            .setParallelPullCount(1)
            .setMaxOutstandingMessagesCount(1L)
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
            .build();
    SourceReader<String, SubscriptionSplit> original = reader(null);
    TestingReaderOutput<String> output = new TestingReaderOutput<>();
    publish("A");
    awaitCount(original, output, 1);
    publish("B");
    pollFor(original, output, Duration.ofSeconds(2));
    assertEquals(List.of("A"), output.getEmittedRecords());
    original.snapshotState(1);
    pollFor(original, output, Duration.ofSeconds(1));
    assertEquals(
        List.of("A"), output.getEmittedRecords()); // Taking a snapshot doesn't free capacity.
    original.notifyCheckpointComplete(1);
    awaitCount(original, output, 2);
    assertEquals(List.of("A", "B"), output.getEmittedRecords());
  }

  @Test(timeout = 40000)
  public void expiredLease_redeliversAndEmitsDuplicateBeforeAnyCheckpointCompletes()
      throws Exception {
    // Use the real reader, emitter and StreamingPull subscriber, explicitly disabling leasing
    // to reproduce the legacy/no-extension failure without a production-scale workload.
    var channel = io.grpc.ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    channels.add(channel);
    var channelProvider =
        com.google.api.gax.rpc.FixedTransportChannelProvider.create(
            com.google.api.gax.grpc.GrpcTransportChannel.create(channel));
    var tracker = new com.google.pubsub.flink.internal.source.reader.PubSubAckTracker();
    SourceReader<String, SubscriptionSplit> original =
        new com.google.pubsub.flink.internal.source.reader.PubSubSourceReader<>(
            PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()),
            tracker,
            ackTracker ->
                new com.google.pubsub.flink.internal.source.reader.PubSubSplitReader(
                    () ->
                        new com.google.pubsub.flink.internal.source.reader
                            .PubSubNotifyingPullSubscriber(
                            receiver ->
                                com.google.cloud.pubsub.v1.Subscriber.newBuilder(
                                        subscription, receiver)
                                    .setChannelProvider(channelProvider)
                                    .setCredentialsProvider(
                                        com.google.api.gax.core.NoCredentialsProvider.create())
                                    .setMaxAckExtensionPeriod(org.threeten.bp.Duration.ZERO)
                                    .build(),
                            ackTracker)),
            new org.apache.flink.configuration.Configuration(),
            mock(SourceReaderContext.class, RETURNS_DEEP_STUBS));
    readers.add(original);
    original.start();
    original.addSplits(
        List.of(
            SubscriptionSplit.create(
                com.google.pubsub.v1.ProjectSubscriptionName.parse(subscription))));
    TestingReaderOutput<String> output = new TestingReaderOutput<>();
    publish("A");
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (output.getEmittedRecords().size() < 2 && System.nanoTime() < deadline) {
      original.pollNext(output);
      Thread.sleep(10);
    }
    assertTrue(
        "No redelivery observed: " + output.getEmittedRecords(),
        output.getEmittedRecords().size() >= 2);
    assertEquals("A", output.getEmittedRecords().get(0));
    assertEquals("A", output.getEmittedRecords().get(1));
    original.snapshotState(1); // Both copies already emitted before the first checkpoint.
    original.notifyCheckpointComplete(1);
  }

  @Test(timeout = 30000)
  public void automaticLeasing_preventsDeadlineRedeliveryDuringDelayedCheckpoint()
      throws Exception {
    SourceReader<String, SubscriptionSplit> original = reader(null);
    TestingReaderOutput<String> output = new TestingReaderOutput<>();
    publish("A");
    awaitCount(original, output, 1);
    pollFor(original, output, Duration.ofSeconds(13)); // Subscription deadline is 10s.
    assertEquals(List.of("A"), output.getEmittedRecords());
    original.snapshotState(1);
    original.notifyCheckpointComplete(1);
    publish("B");
    awaitCount(original, output, 2);
    assertEquals(List.of("A", "B"), output.getEmittedRecords());
  }

  private void pollFor(
      SourceReader<String, SubscriptionSplit> reader,
      TestingReaderOutput<String> output,
      Duration duration)
      throws Exception {
    long deadline = System.nanoTime() + duration.toNanos();
    while (System.nanoTime() < deadline) {
      reader.pollNext(output);
      Thread.sleep(10);
    }
  }

  private SourceReader<String, SubscriptionSplit> reader(List<SubscriptionSplit> splits)
      throws Exception {
    SourceReader<String, SubscriptionSplit> reader =
        source.createReader(mock(SourceReaderContext.class, RETURNS_DEEP_STUBS));
    readers.add(reader);
    reader.start();
    reader.addSplits(
        splits == null
            ? List.of(
                SubscriptionSplit.create(
                    com.google.pubsub.v1.ProjectSubscriptionName.parse(subscription)))
            : splits);
    return reader;
  }

  private List<SubscriptionSplit> roundTrip(List<SubscriptionSplit> splits) throws Exception {
    List<SubscriptionSplit> restored = new ArrayList<>();
    var serializer = source.getSplitSerializer();
    for (SubscriptionSplit split : splits) {
      restored.add(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
    }
    return restored;
  }

  private void awaitCount(
      SourceReader<String, SubscriptionSplit> reader, TestingReaderOutput<String> output, int count)
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (output.getEmittedRecords().size() < count && System.nanoTime() < deadline) {
      reader.pollNext(output);
      Thread.sleep(10);
    }
    assertTrue(
        "Timed out receiving " + count + " records: " + output.getEmittedRecords(),
        output.getEmittedRecords().size() >= count);
  }

  private void publish(String value) throws Exception {
    String data =
        Base64.getEncoder().encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    api("POST", topic + ":publish", "{\"messages\":[{\"data\":\"" + data + "\"}]}");
  }

  private void api(String method, String path, String body) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://" + endpoint + "/v1/" + path))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    var response = http.send(request, HttpResponse.BodyHandlers.ofString());
    assertTrue(response.body(), response.statusCode() >= 200 && response.statusCode() < 300);
  }

  @After
  public void tearDown() throws Exception {
    for (SourceReader<String, SubscriptionSplit> reader : readers) {
      reader.close();
    }
    for (io.grpc.ManagedChannel channel : channels) {
      channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }
    if (endpoint != null && subscription != null) {
      api("DELETE", subscription, "");
      api("DELETE", topic, "");
    }
  }
}

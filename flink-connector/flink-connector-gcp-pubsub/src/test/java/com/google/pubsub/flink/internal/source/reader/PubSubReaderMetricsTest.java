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
package com.google.pubsub.flink.internal.source.reader;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.Test;

public class PubSubReaderMetricsTest {
  private final RecordingGroup group = new RecordingGroup();
  private final AtomicLong clock = new AtomicLong();
  private final PubSubReaderMetrics metrics = new PubSubReaderMetrics(group, clock::get);

  @Test
  public void delayedAck_remainsInFlightUntilItsSdkFutureCompletes() {
    AckReplyConsumerWithResponse consumer = mock(AckReplyConsumerWithResponse.class);
    SettableApiFuture<AckResponse> response = SettableApiFuture.create();
    when(consumer.ack()).thenReturn(response);

    metrics.observe(consumer).ack();
    clock.set(TimeUnit.MILLISECONDS.toNanos(250));

    assertThat(group.count("ack.requests")).isEqualTo(1);
    assertThat(group.value("ack.inFlight")).isEqualTo(1);
    assertThat(group.value("ack.oldestInFlightAgeMillis")).isEqualTo(250L);
    assertThat(group.count("ack.responses.successful")).isEqualTo(0);
    response.set(AckResponse.SUCCESSFUL);
    assertThat(group.value("ack.inFlight")).isEqualTo(0);
    assertThat(group.value("ack.oldestInFlightAgeMillis")).isEqualTo(0L);
    assertThat(group.count("ack.responses.successful")).isEqualTo(1);
    assertThat(group.count("ack.responseFailures")).isEqualTo(0);
  }

  @Test
  public void everyUnsuccessfulResponseAndFutureFailure_isVisibleWithoutThrowingOnReaderThread() {
    AckReplyConsumerWithResponse consumer = mock(AckReplyConsumerWithResponse.class);
    long unsuccessful = 0;
    for (AckResponse response : AckResponse.values()) {
      when(consumer.ack()).thenReturn(ApiFutures.immediateFuture(response));
      metrics.observe(consumer).ack();
      assertThat(group.count("ack.responses." + response.name().toLowerCase(java.util.Locale.ROOT)))
          .isEqualTo(1);
      if (response != AckResponse.SUCCESSFUL) {
        unsuccessful++;
      }
    }
    when(consumer.ack())
        .thenReturn(ApiFutures.immediateFailedFuture(new IllegalStateException("RPC failure")));
    metrics.observe(consumer).ack();
    assertThat(group.count("ack.responseFailures")).isEqualTo(unsuccessful + 1);
    assertThat(group.value("ack.inFlight")).isEqualTo(0);
  }

  @Test
  public void synchronousCallbackFailure_isCountedAndStillPropagated() {
    AckReplyConsumerWithResponse consumer = mock(AckReplyConsumerWithResponse.class);
    IllegalStateException failure = new IllegalStateException("Callback failure");
    when(consumer.ack()).thenThrow(failure);

    assertThat(assertThrows(IllegalStateException.class, () -> metrics.observe(consumer).ack()))
        .isSameInstanceAs(failure);
    assertThat(group.count("ack.callbackFailures")).isEqualTo(1);
    assertThat(group.count("ack.responseFailures")).isEqualTo(0);
    assertThat(group.value("ack.inFlight")).isEqualTo(0);
  }

  @Test
  public void nackOutcomes_areSeparateFromAckOutcomes() {
    AckReplyConsumerWithResponse consumer = mock(AckReplyConsumerWithResponse.class);
    when(consumer.nack()).thenReturn(ApiFutures.immediateFuture(AckResponse.INVALID));
    metrics.observe(consumer).nack();

    assertThat(group.count("nack.requests")).isEqualTo(1);
    assertThat(group.count("nack.responses.invalid")).isEqualTo(1);
    assertThat(group.count("nack.responseFailures")).isEqualTo(1);
    assertThat(group.count("ack.requests")).isEqualTo(0);
  }

  @Test
  public void outOfOrderCompletionAndCancellation_preserveOldestPendingAge() {
    AckReplyConsumerWithResponse consumer = mock(AckReplyConsumerWithResponse.class);
    SettableApiFuture<AckResponse> first = SettableApiFuture.create();
    SettableApiFuture<AckResponse> second = SettableApiFuture.create();
    when(consumer.ack()).thenReturn(first, second);
    AckReplyConsumer observed = metrics.observe(consumer);
    observed.ack();
    clock.set(TimeUnit.MILLISECONDS.toNanos(100));
    observed.ack();
    second.set(AckResponse.SUCCESSFUL);
    clock.set(TimeUnit.MILLISECONDS.toNanos(250));
    assertThat(group.value("ack.inFlight")).isEqualTo(1);
    assertThat(group.value("ack.oldestInFlightAgeMillis")).isEqualTo(250L);
    first.cancel(false);
    assertThat(group.value("ack.inFlight")).isEqualTo(0);
    assertThat(group.count("ack.responseFailures")).isEqualTo(1);
  }

  @Test(timeout = 10000)
  public void concurrentSdkCompletions_doNotLoseCounterUpdates() throws Exception {
    AckReplyConsumerWithResponse consumer = mock(AckReplyConsumerWithResponse.class);
    List<SettableApiFuture<AckResponse>> responses = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      SettableApiFuture<AckResponse> response = SettableApiFuture.create();
      responses.add(response);
      when(consumer.ack()).thenReturn(response);
      metrics.observe(consumer).ack();
    }
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> tasks = new ArrayList<>();
      for (SettableApiFuture<AckResponse> response : responses) {
        tasks.add(executor.submit(() -> response.set(AckResponse.SUCCESSFUL)));
      }
      for (Future<?> task : tasks) {
        task.get(5, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(group.count("ack.responses.successful")).isEqualTo(1000);
    assertThat(group.value("ack.inFlight")).isEqualTo(0);
  }

  @Test
  public void ownershipGauges_followEmissionCheckpointRedeliveryAndShutdown() {
    PubSubAckTracker tracker = new PubSubAckTracker(group, clock::get);
    tracker.addPendingAck("A", mock(AckReplyConsumer.class));
    clock.set(TimeUnit.MILLISECONDS.toNanos(100));
    tracker.addPendingAck("B", mock(AckReplyConsumer.class));
    assertThat(group.value("outstandingMessages")).isEqualTo(2);
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(2);
    assertThat(group.value("pendingMessages")).isEqualTo(2);
    tracker.stagePendingAck("A");
    assertThat(group.value("pendingMessages")).isEqualTo(1);
    assertThat(group.value("stagedMessages")).isEqualTo(1);
    tracker.addCheckpoint(1);
    assertThat(group.value("stagedMessages")).isEqualTo(0);
    assertThat(group.value("checkpointAckBatches")).isEqualTo(1);
    clock.set(TimeUnit.MILLISECONDS.toNanos(250));
    tracker.addPendingAck("A", mock(AckReplyConsumer.class));
    assertThat(group.count("redeliveries")).isEqualTo(1);
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(3);
    assertThat(group.value("oldestOutstandingAgeMillis")).isEqualTo(250L);
    tracker.notifyCheckpointComplete(1);
    assertThat(group.value("outstandingMessages")).isEqualTo(1);
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(1);
    assertThat(group.value("oldestOutstandingAgeMillis")).isEqualTo(150L);
    tracker.nackAll();
    assertThat(group.value("outstandingMessages")).isEqualTo(0);
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(0);
    assertThat(group.value("pendingMessages")).isEqualTo(0);
    assertThat(group.value("checkpointAckBatches")).isEqualTo(0);
    assertThat(group.value("oldestOutstandingAgeMillis")).isEqualTo(0L);
  }

  @Test
  public void failedCheckpointAck_keepsOwnershipGaugesAndFailureCounterAccurate() {
    PubSubAckTracker tracker = new PubSubAckTracker(group, clock::get);
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    org.mockito.Mockito.doThrow(new IllegalStateException("Callback failure")).when(consumer).ack();
    tracker.addPendingAck("A", consumer);
    tracker.stagePendingAck("A");
    tracker.addCheckpoint(1);
    assertThrows(IllegalStateException.class, () -> tracker.notifyCheckpointComplete(1));
    assertThat(group.count("checkpointAckFailures")).isEqualTo(1);
    assertThat(group.value("outstandingMessages")).isEqualTo(1);
    assertThat(group.value("checkpointAckBatches")).isEqualTo(1);
    tracker.nackAll();
    assertThat(group.value("outstandingMessages")).isEqualTo(0);
  }

  @Test
  public void deliveryHandleGauge_deduplicatesIdentityAndRemovesSuccessfulAckPrefix() {
    PubSubAckTracker tracker = new PubSubAckTracker(group, clock::get);
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer latest = mock(AckReplyConsumer.class);
    org.mockito.Mockito.doThrow(new IllegalStateException("Callback failure")).when(original).ack();
    tracker.trackMessageDelivery("A", original);
    tracker.trackMessageDelivery("A", latest);
    tracker.trackMessageDelivery("A", original);
    tracker.trackMessageDelivery("A", latest);
    assertThat(group.value("outstandingMessages")).isEqualTo(1);
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(2);
    tracker.stagePendingAck("A");
    tracker.addCheckpoint(1);
    assertThrows(IllegalStateException.class, () -> tracker.notifyCheckpointComplete(1));
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(1);
    tracker.nackAll();
    assertThat(group.value("outstandingDeliveryHandles")).isEqualTo(0);
  }

  private static final class RecordingGroup extends UnregisteredMetricsGroup {
    private final Map<String, Counter> counters;
    private final Map<String, Gauge<?>> gauges;
    private final String prefix;

    RecordingGroup() {
      this("", new HashMap<>(), new HashMap<>());
    }

    RecordingGroup(String prefix, Map<String, Counter> counters, Map<String, Gauge<?>> gauges) {
      this.prefix = prefix;
      this.counters = counters;
      this.gauges = gauges;
    }

    @Override
    public Counter counter(String name) {
      return counters.computeIfAbsent(prefix + name, unused -> new SimpleCounter());
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      counters.put(prefix + name, counter);
      return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
      gauges.put(prefix + name, gauge);
      return gauge;
    }

    @Override
    public RecordingGroup addGroup(String name) {
      return new RecordingGroup(prefix + name + ".", counters, gauges);
    }

    long count(String name) {
      return counters.get(name).getCount();
    }

    Object value(String name) {
      return gauges.get(name).getValue();
    }
  }
}

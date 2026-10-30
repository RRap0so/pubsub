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
package com.google.pubsub.flink.internal.source.reader;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

public class PubSubAckTracker implements AckTracker {
  @GuardedBy("this")
  private final Map<String, Delivery> outstandingAcks = new LinkedHashMap<>();

  private final LongSupplier nanoTime;
  private final Counter redeliveries;
  private final Counter checkpointAckFailures;

  public PubSubAckTracker() {
    this(new UnregisteredMetricsGroup());
  }

  public PubSubAckTracker(MetricGroup group) {
    this(group, System::nanoTime);
  }

  PubSubAckTracker(MetricGroup group, LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
    redeliveries = group.counter("redeliveries", new ThreadSafeSimpleCounter());
    checkpointAckFailures = group.counter("checkpointAckFailures", new ThreadSafeSimpleCounter());
    group.gauge("outstandingMessages", (Gauge<Integer>) this::outstandingMessages);
    group.gauge("pendingMessages", (Gauge<Integer>) this::pendingMessages);
    group.gauge("stagedMessages", (Gauge<Integer>) this::stagedMessages);
    group.gauge("checkpointAckBatches", (Gauge<Integer>) this::checkpointAckBatches);
    group.gauge("oldestOutstandingAgeMillis", (Gauge<Long>) this::oldestOutstandingAgeMillis);
  }

  @GuardedBy("this")
  private final Set<String> pendingAcks = new HashSet<>();

  @GuardedBy("this")
  private final List<String> stagedAcks = new ArrayList<>();

  @GuardedBy("this")
  private final SortedMap<Long, List<String>> checkpoints = new TreeMap<>();

  @Override
  public synchronized boolean trackMessageDelivery(
      String messageId, AckReplyConsumer ackReplyConsumer) {
    Delivery delivery = outstandingAcks.get(messageId);
    boolean firstDelivery = delivery == null;
    if (firstDelivery) {
      outstandingAcks.put(messageId, new Delivery(ackReplyConsumer, nanoTime.getAsLong()));
      pendingAcks.add(messageId);
    } else {
      delivery.consumer = ackReplyConsumer;
      redeliveries.inc();
    }
    return firstDelivery;
  }

  @Override
  public synchronized void addPendingAck(String messageId, AckReplyConsumer ackReplyConsumer) {
    trackMessageDelivery(messageId, ackReplyConsumer);
  }

  @Override
  public synchronized void stagePendingAck(String messageId) {
    if (pendingAcks.remove(messageId)) {
      stagedAcks.add(messageId);
    }
  }

  @Override
  public synchronized void addCheckpoint(long checkpointId) {
    checkpoints.put(checkpointId, new ArrayList<>(stagedAcks));
    stagedAcks.clear();
  }

  @Override
  public synchronized void notifyCheckpointComplete(long checkpointId) {
    while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
      long completed = checkpoints.firstKey();
      List<String> messages = checkpoints.get(completed);
      int acknowledged = 0;
      try {
        for (String messageId : messages) {
          Delivery delivery = outstandingAcks.get(messageId);
          // Keep ownership until the callback returns. On failure, shutdown can still nack
          // the delivery, or a later completion can retry it using its latest delivery handle.
          delivery.consumer.ack();
          outstandingAcks.remove(messageId);
          acknowledged++;
        }
      } catch (RuntimeException | Error failure) {
        checkpointAckFailures.inc();
        throw failure;
      } finally {
        // Discard the successful prefix in one linear operation, including after partial failure.
        messages.subList(0, acknowledged).clear();
      }
      checkpoints.remove(completed);
    }
  }

  @Override
  public synchronized void nackAll() {
    outstandingAcks.values().forEach(delivery -> delivery.consumer.nack());
    outstandingAcks.clear();
    pendingAcks.clear();
    stagedAcks.clear();
    checkpoints.clear();
  }

  private synchronized int outstandingMessages() {
    return outstandingAcks.size();
  }

  private synchronized int pendingMessages() {
    return pendingAcks.size();
  }

  private synchronized int stagedMessages() {
    return stagedAcks.size();
  }

  private synchronized int checkpointAckBatches() {
    return checkpoints.size();
  }

  private synchronized long oldestOutstandingAgeMillis() {
    return outstandingAcks.isEmpty()
        ? 0
        : TimeUnit.NANOSECONDS.toMillis(
            nanoTime.getAsLong() - outstandingAcks.values().iterator().next().receivedNanos);
  }

  private static final class Delivery {
    AckReplyConsumer consumer;
    final long receivedNanos;

    Delivery(AckReplyConsumer consumer, long receivedNanos) {
      this.consumer = consumer;
      this.receivedNanos = receivedNanos;
    }
  }
}

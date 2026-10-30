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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class PubSubAckTracker implements AckTracker {
  @GuardedBy("this")
  private final Map<String, AckReplyConsumer> outstandingAcks = new HashMap<>();

  @GuardedBy("this")
  private final Set<String> pendingAcks = new HashSet<>();

  @GuardedBy("this")
  private final List<String> stagedAcks = new ArrayList<>();

  @GuardedBy("this")
  private final SortedMap<Long, List<String>> checkpoints = new TreeMap<>();

  @Override
  public synchronized boolean trackMessageDelivery(
      String messageId, AckReplyConsumer ackReplyConsumer) {
    boolean firstDelivery = !outstandingAcks.containsKey(messageId);
    outstandingAcks.put(messageId, ackReplyConsumer);
    if (firstDelivery) {
      pendingAcks.add(messageId);
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
          AckReplyConsumer consumer = outstandingAcks.get(messageId);
          // Keep ownership until the callback returns. On failure, shutdown can still nack
          // the delivery, or a later completion can retry it using its latest delivery handle.
          consumer.ack();
          outstandingAcks.remove(messageId, consumer);
          acknowledged++;
        }
      } finally {
        // Discard the successful prefix in one linear operation, including after partial failure.
        messages.subList(0, acknowledged).clear();
      }
      checkpoints.remove(completed);
    }
  }

  @Override
  public synchronized void nackAll() {
    outstandingAcks.values().forEach((ackReplyConsumer) -> ackReplyConsumer.nack());
    outstandingAcks.clear();
    pendingAcks.clear();
    stagedAcks.clear();
    checkpoints.clear();
  }
}

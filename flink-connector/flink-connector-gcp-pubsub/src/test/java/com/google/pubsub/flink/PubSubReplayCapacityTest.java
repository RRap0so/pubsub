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

import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Repeatable heap-backend capacity probe. Increase -Dpubsub.capacity.messages for a larger run.
 * Measurements exclude network, provider snapshot storage, and production state-backend overhead.
 */
@RunWith(JUnit4.class)
public class PubSubReplayCapacityTest {
  private static final int MAX_PARALLELISM = 16;
  private static final Duration RETENTION = Duration.ofDays(8);

  @Test
  public void checkpointAndRescaleRetainEveryIdUntilExpiry() throws Exception {
    int messages = Integer.getInteger("pubsub.capacity.messages", 2000);
    assertTrue("pubsub.capacity.messages must be positive", messages > 0);
    OperatorSubtaskState checkpoint;
    long started = System.nanoTime();
    long checkpointNanos;
    try (var original = harness(1, 0)) {
      original.open();
      original.setStateTtlProcessingTime(0);
      for (int i = 0; i < messages; i++) {
        original.processElement(record(i));
        assertEquals(1, original.getRecordOutput().size());
        original.getOutput().clear();
      }
      assertEquals(messages, original.numKeyedStateEntries());
      assertEquals(0, original.numProcessingTimeTimers());
      long checkpointStarted = System.nanoTime();
      checkpoint = original.snapshot(1, 0);
      checkpointNanos = System.nanoTime() - checkpointStarted;
    }
    long restoreNanos = 0;
    int expiredAccepted = 0;
    for (int task = 0; task < 2; task++) {
      long restoreStarted = System.nanoTime();
      try (var restored = harness(2, task)) {
        restored.initializeState(
            AbstractStreamOperatorTestHarness.repartitionOperatorState(
                checkpoint, MAX_PARALLELISM, 1, 2, task));
        restored.open();
        restoreNanos += System.nanoTime() - restoreStarted;
        restored.setStateTtlProcessingTime(RETENTION.toMillis() - 1);
        for (int i = 0; i < messages; i++) {
          if (owner(i) == task) {
            restored.processElement(record(i));
          }
        }
        assertTrue("No retained ID may be emitted after rescaling", restored.getOutput().isEmpty());
        restored.setStateTtlProcessingTime(RETENTION.toMillis());
        for (int i = 0; i < messages; i++) {
          if (owner(i) == task) {
            restored.processElement(record(i));
            assertEquals(1, restored.getRecordOutput().size());
            expiredAccepted++;
            restored.getOutput().clear();
          }
        }
      }
    }
    assertEquals(messages, expiredAccepted);
    System.out.printf(
        "pubsubCapacity={\"backend\":\"heap-harness\",\"messages\":%d,"
            + "\"checkpointBytes\":%d,\"checkpointMillis\":%.3f,"
            + "\"restoreMillis\":%.3f,\"totalMillis\":%.3f}%n",
        messages,
        checkpoint.getStateSize(),
        checkpointNanos / 1_000_000.0,
        restoreNanos / 1_000_000.0,
        (System.nanoTime() - started) / 1_000_000.0);
    checkpoint.discardState();
  }

  private static int owner(int id) {
    return KeyGroupRangeAssignment.assignKeyToParallelOperator(key(id), MAX_PARALLELISM, 2);
  }

  private static String key(int id) {
    return "message-" + (1_000_000_000_000_000_000L + id);
  }

  private static StreamRecord<Tuple2<String, byte[]>> record(int id) {
    return new StreamRecord<>(Tuple2.of(key(id), new byte[] {1}));
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          String, Tuple2<String, byte[]>, Tuple2<String, byte[]>>
      harness(int parallelism, int task) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new PubSubReplayDeduplication.Deduplicate(RETENTION)),
        value -> value.f0,
        Types.STRING,
        MAX_PARALLELISM,
        parallelism,
        task);
  }
}

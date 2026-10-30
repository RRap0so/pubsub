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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubAckTrackerTest {
  PubSubAckTracker ackTracker;

  @Before
  public void doBeforeEachTest() {
    ackTracker = new PubSubAckTracker();
  }

  @Test
  public void suppressedRedelivery_retainsEveryHandleUntilCheckpoint() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer latest = mock(AckReplyConsumer.class);
    assertThat(ackTracker.trackMessageDelivery("message-id", original)).isTrue();
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);

    assertThat(ackTracker.trackMessageDelivery("message-id", latest)).isFalse();

    verify(original, never()).nack();
    verify(original, never()).ack();
    verify(latest, never()).ack();
    verify(latest, never()).nack();
    ackTracker.notifyCheckpointComplete(1L);
    ackTracker.nackAll();
    verify(original).ack();
    verify(original, never()).nack();
    verify(latest).ack();
    verify(latest, never()).nack();
  }

  @Test
  public void bothEntryPoints_retainSupersededHandlesUntilShutdown() {
    for (boolean legacyEntryPoint : new boolean[] {false, true}) {
      PubSubAckTracker tracker = new PubSubAckTracker();
      AckReplyConsumer original = mock(AckReplyConsumer.class);
      AckReplyConsumer latest = mock(AckReplyConsumer.class);
      tracker.trackMessageDelivery("message-id", original);
      if (legacyEntryPoint) {
        tracker.addPendingAck("message-id", latest);
      } else {
        assertThat(tracker.trackMessageDelivery("message-id", latest)).isFalse();
      }
      verify(original, never()).ack();
      verify(original, never()).nack();
      verify(latest, never()).ack();
      verify(latest, never()).nack();
      tracker.nackAll();
      verify(original).nack();
      verify(latest).nack();
    }
  }

  @Test
  public void redeliveryBurst_retainsHandlesWithoutNackingAndResolvesAllAtCheckpoint() {
    AtomicInteger outstandingHandles = new AtomicInteger();
    AtomicInteger acknowledgements = new AtomicInteger();
    AtomicInteger negativeAcknowledgements = new AtomicInteger();
    for (int delivery = 0; delivery < 2000; delivery++) {
      outstandingHandles.incrementAndGet();
      AckReplyConsumer consumer =
          new AckReplyConsumer() {
            private boolean resolved;

            @Override
            public void ack() {
              resolve();
              acknowledgements.incrementAndGet();
            }

            @Override
            public void nack() {
              resolve();
              negativeAcknowledgements.incrementAndGet();
            }

            private void resolve() {
              assertThat(resolved).isFalse();
              resolved = true;
              outstandingHandles.decrementAndGet();
            }
          };
      assertThat(ackTracker.trackMessageDelivery("message-id", consumer)).isEqualTo(delivery == 0);
      assertThat(outstandingHandles.get()).isEqualTo(delivery + 1);
      assertThat(acknowledgements.get()).isEqualTo(0);
    }
    assertThat(negativeAcknowledgements.get()).isEqualTo(0);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    ackTracker.nackAll();
    assertThat(outstandingHandles.get()).isEqualTo(0);
    assertThat(acknowledgements.get()).isEqualTo(2000);
    assertThat(negativeAcknowledgements.get()).isEqualTo(0);
  }

  @Test
  public void repeatedHandle_doesNotNackTheHandleRetainedForCheckpoint() {
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    assertThat(ackTracker.trackMessageDelivery("message-id", consumer)).isTrue();
    assertThat(ackTracker.trackMessageDelivery("message-id", consumer)).isFalse();
    verify(consumer, never()).nack();
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(consumer).ack();
  }

  @Test
  public void reusedSupersededHandle_isResolvedOnlyOnce() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer other = mock(AckReplyConsumer.class);
    ackTracker.trackMessageDelivery("message-id", original);
    ackTracker.trackMessageDelivery("message-id", other);
    ackTracker.trackMessageDelivery("message-id", original);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1);
    ackTracker.notifyCheckpointComplete(1);
    ackTracker.nackAll();
    verify(original).ack();
    verify(other).ack();
    verify(original, never()).nack();
    verify(other, never()).nack();
  }

  @Test
  public void supersededAckFailure_doesNotRetrySuccessfullyResolvedHandle() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer latest = mock(AckReplyConsumer.class);
    doThrow(new IllegalStateException("Injected superseded-handle failure"))
        .doNothing()
        .when(original)
        .ack();
    ackTracker.trackMessageDelivery("message-id", original);
    ackTracker.trackMessageDelivery("message-id", latest);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1);
    assertThrows(IllegalStateException.class, () -> ackTracker.notifyCheckpointComplete(1));
    ackTracker.notifyCheckpointComplete(1);
    ackTracker.nackAll();
    verify(latest).ack();
    verify(original, times(2)).ack();
    verify(latest, never()).nack();
    verify(original, never()).nack();
  }

  @Test
  public void supersededAckFailure_keepsOnlyUnresolvedHandleForShutdown() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer latest = mock(AckReplyConsumer.class);
    doThrow(new IllegalStateException("Injected superseded-handle failure")).when(original).ack();
    ackTracker.trackMessageDelivery("message-id", original);
    ackTracker.trackMessageDelivery("message-id", latest);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1);
    assertThrows(IllegalStateException.class, () -> ackTracker.notifyCheckpointComplete(1));
    ackTracker.nackAll();
    verify(latest).ack();
    verify(latest, never()).nack();
    verify(original).nack();
  }

  @Test
  public void partialShutdownFailure_doesNotNackSuccessfullyResolvedHandleAgain() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer latest = mock(AckReplyConsumer.class);
    doThrow(new IllegalStateException("Injected superseded-handle failure"))
        .doNothing()
        .when(original)
        .nack();
    ackTracker.trackMessageDelivery("message-id", original);
    ackTracker.trackMessageDelivery("message-id", latest);
    assertThrows(IllegalStateException.class, ackTracker::nackAll);
    ackTracker.nackAll();
    verify(latest).nack();
    verify(original, times(2)).nack();
    verify(latest, never()).ack();
    verify(original, never()).ack();
  }

  @Test
  public void singleAck_ackedOnCheckpoint() throws Exception {
    AckReplyConsumer mockAck = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", mockAck);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck).ack();
  }

  @Test
  public void checkpointAckFailure_keepsFailedDeliveryAvailableForShutdown() {
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    doThrow(new IllegalStateException("Injected acknowledgement callback failure"))
        .when(consumer)
        .ack();
    ackTracker.addPendingAck("message-id", consumer);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);

    assertThrows(IllegalStateException.class, () -> ackTracker.notifyCheckpointComplete(1L));
    ackTracker.nackAll();

    // A failed callback must not make the delivery disappear from shutdown cleanup.
    // This injects a synchronous failure; it does not model a broker acknowledgement response.
    verify(consumer).nack();
  }

  @Test
  public void partialCheckpointAckFailure_retriesOnlyRemainingIdsUsingLatestDelivery() {
    AckReplyConsumer first = mock(AckReplyConsumer.class);
    AckReplyConsumer failed = mock(AckReplyConsumer.class);
    AckReplyConsumer latest = mock(AckReplyConsumer.class);
    AckReplyConsumer last = mock(AckReplyConsumer.class);
    doThrow(new IllegalStateException("Injected acknowledgement callback failure"))
        .doNothing()
        .when(failed)
        .ack();
    ackTracker.addPendingAck("first", first);
    ackTracker.stagePendingAck("first");
    ackTracker.addPendingAck("failed", failed);
    ackTracker.stagePendingAck("failed");
    ackTracker.addPendingAck("last", last);
    ackTracker.stagePendingAck("last");
    ackTracker.addCheckpoint(1L);

    assertThrows(IllegalStateException.class, () -> ackTracker.notifyCheckpointComplete(1L));
    verify(first).ack();
    verify(last, never()).ack();
    assertThat(ackTracker.trackMessageDelivery("failed", latest)).isFalse();
    ackTracker.notifyCheckpointComplete(1L);
    ackTracker.notifyCheckpointComplete(1L); // Repeated completion must be harmless.
    ackTracker.nackAll();

    verify(first, times(1)).ack();
    verify(failed, times(2)).ack();
    verify(latest, times(1)).ack();
    verify(last, times(1)).ack();
    verify(first, never()).nack();
    verify(failed, never()).nack();
    verify(latest, never()).nack();
    verify(last, never()).nack();
  }

  @Test
  public void singleAck_ackEveryMessageDeliveryOnCheckpoint() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", mockAck1);
    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", mockAck2);

    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck1, times(1)).ack();
    verify(mockAck2, times(1)).ack();
  }

  @Test
  public void redeliveryAfterSnapshot_ackEveryDeliveryOnOriginalCheckpoint() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer redelivery = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", original);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);

    ackTracker.addPendingAck("message-id", redelivery);
    ackTracker.notifyCheckpointComplete(1L);

    verify(original).ack();
    verify(redelivery).ack();
    ackTracker.nackAll();
    verify(redelivery, times(0)).nack();
  }

  @Test
  public void redeliveryAfterEmission_nacksSupersededHandleAndLatestOnShutdown() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer redelivery = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", original);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addPendingAck("message-id", redelivery);

    ackTracker.nackAll();

    verify(original).nack();
    verify(redelivery).nack();
  }

  @Test
  public void redeliveryAfterEmission_ackEveryDeliveryWhenCheckpointCompletes() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer redelivery = mock(AckReplyConsumer.class);
    assertThat(ackTracker.trackMessageDelivery("message-id", original)).isTrue();
    ackTracker.stagePendingAck("message-id");
    assertThat(ackTracker.trackMessageDelivery("message-id", redelivery)).isFalse();
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);

    verify(original).ack();
    verify(redelivery).ack();
  }

  @Test
  public void completedCheckpoint_releasesMessageIdForFutureDelivery() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    assertThat(ackTracker.trackMessageDelivery("message-id", original)).isTrue();
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);

    assertThat(ackTracker.trackMessageDelivery("message-id", mock(AckReplyConsumer.class)))
        .isTrue();
  }

  @Test
  public void shutdown_releasesMessageIdForFutureDelivery() {
    assertThat(ackTracker.trackMessageDelivery("message-id", mock(AckReplyConsumer.class)))
        .isTrue();
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.nackAll();

    assertThat(ackTracker.trackMessageDelivery("message-id", mock(AckReplyConsumer.class)))
        .isTrue();
  }

  @Test
  public void manyAcks_ackedOnCheckpoint() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message1-id", mockAck1);
    ackTracker.stagePendingAck("message1-id");
    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message2-id", mockAck2);
    ackTracker.stagePendingAck("message2-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck1).ack();
    verify(mockAck2).ack();
  }

  @Test
  public void manyCheckpoints_completedOneByOne() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message1-id", mockAck1);
    ackTracker.stagePendingAck("message1-id");
    ackTracker.addCheckpoint(1L);

    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message2-id", mockAck2);
    ackTracker.stagePendingAck("message2-id");
    ackTracker.addCheckpoint(2L);

    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck1, times(1)).ack();
    verify(mockAck2, times(0)).ack();

    ackTracker.notifyCheckpointComplete(2L);
    verify(mockAck1, times(1)).ack();
    verify(mockAck2, times(1)).ack();
  }

  @Test
  public void manyCheckpoints_completedTogether() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message1-id", mockAck1);
    ackTracker.stagePendingAck("message1-id");
    ackTracker.addCheckpoint(1L);

    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message2-id", mockAck2);
    ackTracker.stagePendingAck("message2-id");
    ackTracker.addCheckpoint(2L);

    ackTracker.notifyCheckpointComplete(2L);
    verify(mockAck1).ack();
    verify(mockAck2).ack();
  }

  @Test
  public void nackAll_pendingStagedAndIncompleteCheckpointAcks() throws Exception {
    // mockAck1 is added to checkpoint 1, which never completes.
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message1-id", mockAck1);
    ackTracker.stagePendingAck("message1-id");
    ackTracker.addCheckpoint(1L);
    // mockAck2 is staged.
    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message2-id", mockAck2);
    ackTracker.stagePendingAck("message2-id");
    // mockAck3 is pending.
    AckReplyConsumer mockAck3 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message3-id", mockAck3);

    ackTracker.nackAll();

    verify(mockAck1).nack();
    verify(mockAck2).nack();
    verify(mockAck3).nack();
  }
}

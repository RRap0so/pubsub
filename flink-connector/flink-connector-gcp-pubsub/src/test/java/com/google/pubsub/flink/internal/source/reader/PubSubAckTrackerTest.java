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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
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
  public void singleAck_ackedOnCheckpoint() throws Exception {
    AckReplyConsumer mockAck = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", mockAck);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck).ack();
  }

  @Test
  public void singleAck_ackLatestMessageDelivery() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", mockAck1);
    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", mockAck2);

    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck1, times(0)).ack();
    verify(mockAck2, times(1)).ack();
  }

  @Test
  public void redeliveryAfterSnapshot_ackLatestDeliveryOnOriginalCheckpoint() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer redelivery = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", original);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addCheckpoint(1L);

    ackTracker.addPendingAck("message-id", redelivery);
    ackTracker.notifyCheckpointComplete(1L);

    verify(original, times(0)).ack();
    verify(redelivery).ack();
    ackTracker.nackAll();
    verify(redelivery, times(0)).nack();
  }

  @Test
  public void redeliveryAfterEmission_nackOnlyLatestDeliveryOnShutdown() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer redelivery = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck("message-id", original);
    ackTracker.stagePendingAck("message-id");
    ackTracker.addPendingAck("message-id", redelivery);

    ackTracker.nackAll();

    verify(original, times(0)).nack();
    verify(redelivery).nack();
  }

  @Test
  public void redeliveryAfterEmission_ackLatestDeliveryWhenCheckpointCompletes() {
    AckReplyConsumer original = mock(AckReplyConsumer.class);
    AckReplyConsumer redelivery = mock(AckReplyConsumer.class);
    assertThat(ackTracker.trackMessageDelivery("message-id", original)).isTrue();
    ackTracker.stagePendingAck("message-id");
    assertThat(ackTracker.trackMessageDelivery("message-id", redelivery)).isFalse();
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);

    verify(original, times(0)).ack();
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

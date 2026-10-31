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
package com.google.pubsub.flink.internal.sink;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Uses the actual SDK and a local gRPC service that stalls or fails publish RPCs. */
public class PubSubPublisherShutdownIT {
  @Test(timeout = 30000)
  public void repeatedStalledCloses_doNotAccumulateWorkersChannelsOrExecutors() throws Exception {
    verifyStalledCloses(false, false);
  }

  @Test(timeout = 30000)
  public void repeatedOrderedStalledCloses_alsoReleaseQueuedPublishes() throws Exception {
    verifyStalledCloses(true, false);
  }

  @Test(timeout = 30000)
  public void repeatedClosesDuringRetryBackoff_doNotStrandShutdownWaiters() throws Exception {
    verifyStalledCloses(false, true);
  }

  private void verifyStalledCloses(boolean ordered, boolean retryBackoff) throws Exception {
    LinkedBlockingQueue<PublishRequest> requests = new LinkedBlockingQueue<>();
    Server server =
        ServerBuilder.forPort(0)
            .directExecutor()
            .addService(
                new PublisherGrpc.PublisherImplBase() {
                  @Override
                  public void publish(
                      PublishRequest request, StreamObserver<PublishResponse> response) {
                    requests.add(request);
                    if (retryBackoff) {
                      response.onError(Status.UNAVAILABLE.asRuntimeException());
                    } // Otherwise deliberately leave the RPC in flight.
                  }
                })
            .build()
            .start();
    List<ManagedChannel> channels = new ArrayList<>();
    List<ScheduledThreadPoolExecutor> executors = new ArrayList<>();
    List<Thread> workers = new ArrayList<>();
    try {
      for (int restart = 0; restart < 5; restart++) {
        CountDownLatch retryQueued = new CountDownLatch(1);
        ScheduledThreadPoolExecutor executor =
            new PubSubPublisherExecutor(2, Executors.defaultThreadFactory()) {
              @Override
              protected void afterExecute(Runnable runnable, Throwable failure) {
                super.afterExecute(runnable, failure);
                for (Runnable task : getQueue()) {
                  if (task instanceof Delayed && ((Delayed) task).getDelay(TimeUnit.SECONDS) > 5) {
                    retryQueued.countDown();
                  }
                }
              }
            };
        executors.add(executor);
        ScheduledThreadPoolExecutor callbackExecutor = new ScheduledThreadPoolExecutor(2);
        executors.add(callbackExecutor);
        ManagedChannel channel =
            ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .executor(callbackExecutor)
                .usePlaintext()
                .build();
        channels.add(channel);
        GrpcTransportChannel transport = GrpcTransportChannel.create(channel);
        Publisher.Builder sdkBuilder =
            Publisher.newBuilder("projects/shutdown-test/topics/restart-" + restart)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setChannelProvider(FixedTransportChannelProvider.create(transport))
                .setExecutorProvider(FixedExecutorProvider.create(executor))
                .setEnableMessageOrdering(ordered)
                .setBatchingSettings(
                    Publisher.Builder.getDefaultBatchingSettings().toBuilder()
                        .setElementCountThreshold(1L)
                        .build());
        if (retryBackoff) {
          sdkBuilder.setRetrySettings(
              PublisherStubSettings.newBuilder().publishSettings().getRetrySettings().toBuilder()
                  .setInitialRetryDelay(org.threeten.bp.Duration.ofSeconds(30))
                  .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(30))
                  .build());
        }
        Publisher sdk = sdkBuilder.build();
        PubSubFlushablePublisher publisher =
            new PubSubFlushablePublisher(
                sdk, transport, executor, callbackExecutor, 100, TimeUnit.MILLISECONDS);
        PubsubMessage message =
            PubsubMessage.newBuilder().setOrderingKey(ordered ? "key" : "").build();
        publisher.publish(message);
        assertThat(requests.poll(5, TimeUnit.SECONDS)).isNotNull();
        if (retryBackoff) assertThat(retryQueued.await(5, TimeUnit.SECONDS)).isTrue();
        if (ordered) {
          // This batch waits behind the first one inside the SDK's per-key executor.
          publisher.publish(message);
        }
        assertThrows(IOException.class, publisher::close);
        publisher.close();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
          if (thread.getName().startsWith("pubsub-publisher-close")) workers.add(thread);
        }
        assertThat(channel.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackExecutor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        for (Thread worker : workers) {
          worker.join(3000);
          assertThat(worker.isAlive()).isFalse();
        }
      }
    } finally {
      for (ManagedChannel channel : channels) channel.shutdownNow();
      for (ScheduledThreadPoolExecutor executor : executors) executor.shutdownNow();
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      for (Thread worker : workers) worker.join(3000);
    }
  }
}

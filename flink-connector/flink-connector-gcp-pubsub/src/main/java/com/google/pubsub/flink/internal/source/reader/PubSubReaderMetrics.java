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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;

/**
 * Observes SDK acknowledgement results without blocking the reader or changing checkpoint
 * ownership. Without exactly-once delivery, the SDK reports immediate success; that is not broker
 * confirmation.
 */
public final class PubSubReaderMetrics {
  private final OperationMetrics acknowledgements;
  private final OperationMetrics negativeAcknowledgements;

  public PubSubReaderMetrics(MetricGroup group) {
    this(group, System::nanoTime);
  }

  PubSubReaderMetrics(MetricGroup group, LongSupplier nanoTime) {
    acknowledgements = new OperationMetrics(group.addGroup("ack"), nanoTime);
    negativeAcknowledgements = new OperationMetrics(group.addGroup("nack"), nanoTime);
  }

  public AckReplyConsumer observe(AckReplyConsumerWithResponse consumer) {
    return new AckReplyConsumer() {
      @Override
      public void ack() {
        acknowledgements.observe(consumer::ack);
      }

      @Override
      public void nack() {
        negativeAcknowledgements.observe(consumer::nack);
      }
    };
  }

  // SDK callbacks can run concurrently. Result accounting and pending-operation removal share
  // this lock; thread-safe counters also publish updates to metric reporter threads.
  private static final class OperationMetrics {
    private final Counter requests;
    private final Counter callbackFailures;
    private final Counter responseFailures;
    private final Map<AckResponse, Counter> responses = new EnumMap<>(AckResponse.class);
    private final Map<Long, Long> pending = new LinkedHashMap<>();
    private final LongSupplier nanoTime;
    private long nextOperation;

    OperationMetrics(MetricGroup group, LongSupplier nanoTime) {
      this.nanoTime = nanoTime;
      requests = group.counter("requests", new ThreadSafeSimpleCounter());
      callbackFailures = group.counter("callbackFailures", new ThreadSafeSimpleCounter());
      responseFailures = group.counter("responseFailures", new ThreadSafeSimpleCounter());
      MetricGroup responseGroup = group.addGroup("responses");
      for (AckResponse response : AckResponse.values()) {
        responses.put(
            response,
            responseGroup.counter(
                response.name().toLowerCase(Locale.ROOT), new ThreadSafeSimpleCounter()));
      }
      group.gauge("inFlight", (Gauge<Integer>) this::inFlight);
      group.gauge("oldestInFlightAgeMillis", (Gauge<Long>) this::oldestInFlightAgeMillis);
    }

    void observe(Supplier<ApiFuture<AckResponse>> operation) {
      long id;
      synchronized (this) {
        requests.inc();
        id = nextOperation++;
        pending.put(id, nanoTime.getAsLong());
      }
      ApiFuture<AckResponse> future;
      try {
        future = operation.get();
      } catch (RuntimeException | Error failure) {
        synchronized (this) {
          pending.remove(id);
          callbackFailures.inc();
        }
        throw failure;
      }
      ApiFutures.addCallback(
          future,
          new ApiFutureCallback<AckResponse>() {
            @Override
            public void onSuccess(AckResponse response) {
              finish(id, response);
            }

            @Override
            public void onFailure(Throwable failure) {
              finish(id, null);
            }
          },
          MoreExecutors.directExecutor());
    }

    private synchronized void finish(long id, AckResponse response) {
      pending.remove(id);
      if (response != null) {
        responses.get(response).inc();
      }
      if (response != AckResponse.SUCCESSFUL) {
        responseFailures.inc();
      }
    }

    private synchronized int inFlight() {
      return pending.size();
    }

    private synchronized long oldestInFlightAgeMillis() {
      return pending.isEmpty()
          ? 0
          : TimeUnit.NANOSECONDS.toMillis(
              nanoTime.getAsLong() - pending.values().iterator().next());
    }
  }
}

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
package com.google.pubsub.flink.internal.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.TransportChannel;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publisher owned by one sink writer. Flink serializes write, flush, and close on the task thread.
 * Close hands the outstanding-publish list to a cleanup worker; callers must not use this instance
 * afterward. This class does not support concurrent publishing and flushing.
 */
public class PubSubFlushablePublisher implements FlushablePublisher {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubFlushablePublisher.class);
  private static final long DEFAULT_CLOSE_TIMEOUT_SECONDS = 30;
  private final Publisher publisher;
  private final TransportChannel transport;
  private final ScheduledExecutorService executor;
  private final ScheduledExecutorService callbackExecutor;
  private final List<ApiFuture<String>> outstandingPublishes = new ArrayList<>();
  private final long closeTimeoutNanos;
  private volatile boolean closed;

  public PubSubFlushablePublisher(Publisher publisher) {
    this(publisher, DEFAULT_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  public PubSubFlushablePublisher(
      Publisher publisher,
      TransportChannel transport,
      ScheduledExecutorService executor,
      ScheduledExecutorService callbackExecutor) {
    this(
        publisher,
        transport,
        executor,
        callbackExecutor,
        DEFAULT_CLOSE_TIMEOUT_SECONDS,
        TimeUnit.SECONDS);
  }

  PubSubFlushablePublisher(Publisher publisher, long closeTimeout, TimeUnit unit) {
    this(publisher, null, null, null, closeTimeout, unit);
  }

  PubSubFlushablePublisher(
      Publisher publisher,
      TransportChannel transport,
      ScheduledExecutorService executor,
      ScheduledExecutorService callbackExecutor,
      long closeTimeout,
      TimeUnit unit) {
    if (closeTimeout <= 0) {
      throw new IllegalArgumentException("Close timeout must be positive");
    }
    this.publisher = publisher;
    this.transport = transport;
    this.executor = executor;
    this.callbackExecutor = callbackExecutor;
    this.closeTimeoutNanos = unit.toNanos(closeTimeout);
  }

  @Override
  public void publish(PubsubMessage message) throws InterruptedException {
    if (closed) {
      throw new IllegalStateException("Publisher is closed");
    }
    outstandingPublishes.add(publisher.publish(message));
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("Publisher is closed");
    }
    flushOutstanding();
  }

  private void flushOutstanding() throws IOException {
    publisher.publishAllOutstanding();
    try {
      ApiFutures.allAsList(outstandingPublishes).get();
      outstandingPublishes.clear();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for Pub/Sub publishes", interrupted);
    } catch (ExecutionException failed) {
      throw new IOException("Pub/Sub publish failed", failed.getCause());
    } catch (CancellationException cancelled) {
      throw new IOException("Pub/Sub publish was cancelled", cancelled);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    long closeStarted = System.nanoTime();
    AtomicBoolean detached = new AtomicBoolean();
    AtomicBoolean reported = new AtomicBoolean();
    String topic = publisher.getTopicNameString();
    String topicId = topic == null ? "unknown" : topic.substring(topic.lastIndexOf('/') + 1);
    String workerName = "pubsub-publisher-close-" + topicId + "-" + UUID.randomUUID();
    FutureTask<Void> cleanup =
        new FutureTask<Void>(
            () -> {
              closePublisher(closeStarted);
              return null;
            }) {
          @Override
          protected void done() {
            reportDetachedResult(this, detached, reported, workerName);
          }
        };
    Thread worker = new Thread(cleanup, workerName);
    // SDK shutdown may ignore interrupts. On timeout, also abort the transport and executor
    // owned by this writer, so pending publish RPCs fail and release the SDK's shutdown waiter.
    worker.setDaemon(true);
    worker.start();
    try {
      cleanup.get(remainingNanos(closeStarted), TimeUnit.NANOSECONDS);
    } catch (TimeoutException timedOut) {
      IOException failure =
          new IOException(
              "Pub/Sub publisher close did not finish within "
                  + TimeUnit.NANOSECONDS.toMillis(closeTimeoutNanos)
                  + " ms",
              timedOut);
      detachCleanup(worker, cleanup, detached, reported, closeStarted, failure);
      throw failure;
    } catch (InterruptedException interrupted) {
      IOException failure =
          new IOException("Interrupted while closing Pub/Sub publisher", interrupted);
      detachCleanup(worker, cleanup, detached, reported, closeStarted, failure);
      Thread.currentThread().interrupt();
      throw failure;
    } catch (ExecutionException failed) {
      Throwable cause = failed.getCause();
      forceResources(cause);
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new IOException("Unable to close Pub/Sub publisher", cause);
    }
  }

  private void closePublisher(long closeStarted) throws IOException {
    IOException failure = null;
    try {
      flushOutstanding();
    } catch (IOException failed) {
      failure = failed;
    } catch (RuntimeException failed) {
      failure = new IOException("Unable to flush Pub/Sub publisher during close", failed);
    } finally {
      try {
        try {
          shutdownPublisher(closeStarted);
        } finally {
          // Cancellation callbacks must stay runnable until shutdown's pending-publish waiter
          // finishes. This executor is separate from the retry scheduler aborted by the caller.
          if (callbackExecutor != null) callbackExecutor.shutdownNow();
        }
      } catch (IOException cleanupFailure) {
        if (failure == null) {
          failure = cleanupFailure;
        } else {
          failure.addSuppressed(cleanupFailure);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private void shutdownPublisher(long closeStarted) throws IOException {
    try {
      publisher.shutdown();
      // Fixed providers leave these resources to the writer. Any failure below is also followed
      // by forced cleanup in close(), preserving the original SDK or publish failure.
      if (transport != null) transport.shutdown();
      if (executor != null) executor.shutdown();
      if (callbackExecutor != null) callbackExecutor.shutdown();
      if (!publisher.awaitTermination(remainingNanos(closeStarted), TimeUnit.NANOSECONDS)
          || (transport != null
              && !transport.awaitTermination(remainingNanos(closeStarted), TimeUnit.NANOSECONDS))
          || (executor != null
              && !executor.awaitTermination(remainingNanos(closeStarted), TimeUnit.NANOSECONDS))
          || (callbackExecutor != null
              && !callbackExecutor.awaitTermination(
                  remainingNanos(closeStarted), TimeUnit.NANOSECONDS))) {
        throw new IOException("Pub/Sub publisher did not terminate within the close budget");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while closing Pub/Sub publisher", interrupted);
    } catch (RuntimeException failure) {
      throw new IOException("Unable to shut down Pub/Sub publisher", failure);
    }
  }

  private long remainingNanos(long closeStarted) {
    return Math.max(0, closeTimeoutNanos - (System.nanoTime() - closeStarted));
  }

  private void detachCleanup(
      Thread worker,
      FutureTask<Void> cleanup,
      AtomicBoolean detached,
      AtomicBoolean reported,
      long closeStarted,
      IOException failure) {
    detached.set(true);
    LOG.warn(
        "Publisher cleanup worker {} is still draining after {} ms; aborting owned resources",
        worker.getName(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStarted));
    // Do not cancel the FutureTask: its shutdown finally block must run even if not started yet.
    worker.interrupt();
    forceResources(failure);
    reportDetachedResult(cleanup, detached, reported, worker.getName());
  }

  private void forceResources(Throwable failure) {
    // Callback execution must survive until the SDK shutdown waiter drains. Construction failure,
    // in contrast, can close both executors immediately because no publishes were handed off.
    forceCloseResources(failure, transport, executor);
  }

  /** Aborts the supplied owned resources, preserving the primary failure and all cleanup errors. */
  @Internal
  public static void forceCloseResources(
      Throwable failure, TransportChannel transport, ExecutorService... executors) {
    try {
      if (transport != null) transport.shutdownNow();
    } catch (RuntimeException cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    } finally {
      for (ExecutorService executor : executors) {
        try {
          if (executor != null) executor.shutdownNow();
        } catch (RuntimeException cleanupFailure) {
          if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
        }
      }
    }
  }

  private static void reportDetachedResult(
      FutureTask<Void> cleanup, AtomicBoolean detached, AtomicBoolean reported, String workerName) {
    if (!detached.get() || !cleanup.isDone() || !reported.compareAndSet(false, true)) return;
    try {
      cleanup.get();
      LOG.info("Publisher cleanup worker {} finished after the caller stopped waiting", workerName);
    } catch (ExecutionException failure) {
      LOG.warn(
          "Publisher cleanup worker {} failed after the caller stopped waiting",
          workerName,
          failure.getCause());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while reporting publisher cleanup worker {}", workerName, interrupted);
    }
  }
}

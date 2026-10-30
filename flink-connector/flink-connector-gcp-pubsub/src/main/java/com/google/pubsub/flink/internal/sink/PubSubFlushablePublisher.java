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
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PubSubFlushablePublisher implements FlushablePublisher {
  private final Publisher publisher;
  private final List<ApiFuture<String>> outstandingPublishes = new ArrayList<>();
  private boolean closed;

  public PubSubFlushablePublisher(Publisher publisher) {
    this.publisher = publisher;
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
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      flush();
    } catch (IOException failed) {
      failure = failed;
    } catch (RuntimeException failed) {
      failure = new IOException("Unable to flush Pub/Sub publisher during close", failed);
    } finally {
      try {
        shutdownPublisher();
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

  private void shutdownPublisher() throws IOException {
    try {
      // SDK shutdown itself drains pending requests. Bound the subsequent resource wait.
      publisher.shutdown();
      if (!publisher.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new IOException("Pub/Sub publisher did not terminate within 30 seconds");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while closing Pub/Sub publisher", interrupted);
    } catch (RuntimeException failure) {
      throw new IOException("Unable to shut down Pub/Sub publisher", failure);
    }
  }
}

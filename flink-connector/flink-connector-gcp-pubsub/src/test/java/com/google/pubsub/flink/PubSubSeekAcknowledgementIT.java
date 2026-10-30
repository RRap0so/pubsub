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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.common.base.Optional;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.flink.internal.source.enumerator.PubSubSnapshotStore;
import com.google.pubsub.flink.internal.source.reader.PubSubAckTracker;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Broker-boundary reproduction, not a Flink restart or SDK shutdown test. Three known IDs
 * distinguish an ack before seek, a delayed old-delivery ack after seek, and an unacked
 * lease-expiry control.
 *
 * <p>Run explicitly with -Dtest=PubSubSeekAcknowledgementIT. Uses CHECKPOINT_TEST_EMULATOR when
 * set; otherwise PUBSUB_SEEK_TEST_PROJECT must explicitly select a disposable cloud project, using
 * ADC. Cloud failures retain the named resources for inspection. A bounded timeout demonstrates
 * missing delivery within that window, not permanent loss. Emulator results do not establish cloud
 * semantics.
 */
public class PubSubSeekAcknowledgementIT {
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private String base;
  private String project;
  private String emulator;
  private GoogleCredentials credentials;

  @Before
  public void setUp() throws Exception {
    emulator = System.getenv("CHECKPOINT_TEST_EMULATOR");
    if (emulator != null) {
      assertTrue(emulator.matches("127\\.0\\.0\\.1:[0-9]+"));
      project = "checkpoint-repro";
      base = "http://" + emulator + "/v1/";
    } else {
      project = System.getenv("PUBSUB_SEEK_TEST_PROJECT");
      Assume.assumeNotNull(project);
      assertTrue(project.matches("[a-z][a-z0-9-]{4,61}[a-z0-9]"));
      base = "https://pubsub.googleapis.com/v1/";
      credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped("https://www.googleapis.com/auth/pubsub");
    }
  }

  @Test(timeout = 360000)
  public void standardSubscription_oldAckAfterSeek_doesNotHideReplay() throws Exception {
    verifyReplay(false);
  }

  @Test(timeout = 360000)
  public void exactlyOnceSubscription_oldAckAfterSeek_doesNotHideReplay() throws Exception {
    Assume.assumeTrue("Exactly-once delivery requires the real service", emulator == null);
    verifyReplay(true);
  }

  private void verifyReplay(boolean exactlyOnce) throws Exception {
    String suffix = "seek-repro-" + UUID.randomUUID();
    String root = "projects/" + project;
    String topic = root + "/topics/" + suffix;
    String subscription = root + "/subscriptions/" + suffix;
    String snapshot = root + "/snapshots/" + suffix;
    String auditSubscription = subscription + "-audit";
    boolean passed = false;
    try (PubSubSnapshotStore store =
        new PubSubSnapshotStore(
            project,
            suffix,
            Optional.fromNullable(credentials),
            emulator == null ? Optional.absent() : Optional.of("emulator:///" + emulator))) {
      api("PUT", topic, "{}");
      api(
          "PUT",
          subscription,
          "{\"topic\":\""
              + topic
              + "\",\"ackDeadlineSeconds\":"
              + (emulator == null ? 120 : 10)
              + ",\"enableExactlyOnceDelivery\":"
              + exactlyOnce
              + "}");
      store.create(suffix); // Snapshot precedes all three publications.
      PublishResponse.Builder publication = PublishResponse.newBuilder();
      JsonFormat.parser()
          .merge(
              api(
                  "POST",
                  topic + ":publish",
                  "{\"messages\":[{\"data\":\"QQ==\"},{\"data\":\"Qg==\"},{\"data\":\"Qw==\"}]}"),
              publication);
      List<String> ids = publication.getMessageIdsList();
      assertEquals(3, ids.size());
      System.out.println(
          "SEEK_REPRO published="
              + ids
              + " subscription="
              + subscription
              + " snapshot="
              + snapshot
              + " exactlyOnce="
              + exactlyOnce);

      Map<String, String> deliveries = initialDeliveries(subscription, new HashSet<>(ids));
      PubSubAckTracker tracker = new PubSubAckTracker();
      List<String> queuedAcks = new ArrayList<>();
      tracker.addPendingAck(ids.get(0), consumer(subscription, deliveries.get(ids.get(0)), null));
      tracker.stagePendingAck(ids.get(0));
      tracker.addCheckpoint(1);
      tracker.notifyCheckpointComplete(1); // A is acknowledged before rollback.
      tracker.addPendingAck(
          ids.get(1), consumer(subscription, deliveries.get(ids.get(1)), queuedAcks));
      tracker.stagePendingAck(ids.get(1));
      tracker.addCheckpoint(2);
      tracker.notifyCheckpointComplete(2); // B's ack is queued, but not sent yet.
      assertEquals(1, queuedAcks.size());
      // C remains leased without an ack or lease extension: a positive expiry control.
      store.seek(snapshot); // Exercise the connector's actual administrative seek path.
      if (emulator == null) {
        System.out.println("SEEK_REPRO waiting 70s for documented seek convergence");
        Thread.sleep(70000);
      }
      int status = sendAck(subscription, queuedAcks.get(0));
      // An HTTP success alone does not prove the stale ack affected broker acknowledgement state.
      System.out.println("SEEK_REPRO staleAckHttpStatus=" + status);
      Set<String> replayed = collect(subscription, new HashSet<>(ids), emulator == null ? 150 : 20);
      Set<String> missing = new HashSet<>(ids);
      missing.removeAll(replayed);
      System.out.println("SEEK_REPRO replayed=" + replayed + " missing=" + missing);
      if (!missing.isEmpty()) {
        // Apply the very same snapshot to a fresh subscription, separating snapshot coverage
        // from acknowledgement/lease state on the restored subscription.
        api("PUT", auditSubscription, "{\"topic\":\"" + topic + "\"}");
        api("POST", auditSubscription + ":seek", "{\"snapshot\":\"" + snapshot + "\"}");
        Set<String> audit =
            collect(auditSubscription, new HashSet<>(ids), emulator == null ? 80 : 15);
        System.out.println("SEEK_REPRO freshSubscriptionReplay=" + audit);
      }
      assertEquals(
          "Replay missing IDs; A=ack-before-seek, B=old-ack-after-seek, C=no-ack: " + ids,
          new HashSet<>(ids),
          replayed);
      passed = true;
    } finally {
      if (passed || emulator != null) {
        // Delete only this test's explicitly named resources, including partial setup.
        delete(auditSubscription);
        delete(snapshot);
        delete(subscription);
        delete(topic);
      } else {
        System.out.println(
            "SEEK_REPRO retained topic="
                + topic
                + " subscription="
                + subscription
                + " snapshot="
                + snapshot
                + " auditSubscription="
                + auditSubscription);
      }
    }
  }

  private Map<String, String> initialDeliveries(String subscription, Set<String> expected)
      throws Exception {
    Map<String, String> result = new HashMap<>();
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (!result.keySet().containsAll(expected) && System.nanoTime() < deadline) {
      for (ReceivedMessage message : pull(subscription)) {
        result.put(message.getMessage().getMessageId(), message.getAckId());
      }
      if (!result.keySet().containsAll(expected)) {
        Thread.sleep(100);
      }
    }
    assertEquals("Initial delivery incomplete", expected, result.keySet());
    return result;
  }

  private Set<String> collect(String subscription, Set<String> expected, int seconds)
      throws Exception {
    Set<String> result = new HashSet<>();
    long started = System.nanoTime();
    long deadline = started + Duration.ofSeconds(seconds).toNanos();
    while (!result.containsAll(expected) && System.nanoTime() < deadline) {
      for (ReceivedMessage message : pull(subscription)) {
        String id = message.getMessage().getMessageId();
        if (result.add(id)) {
          System.out.println(
              "SEEK_REPRO delivered="
                  + id
                  + " elapsedSeconds="
                  + Duration.ofNanos(System.nanoTime() - started).toSeconds());
        }
        int status = sendAck(subscription, message.getAckId());
        assertTrue("Current delivery ack failed: HTTP " + status, status >= 200 && status < 300);
      }
      if (!result.containsAll(expected)) {
        Thread.sleep(100);
      }
    }
    return result;
  }

  private List<ReceivedMessage> pull(String subscription) throws Exception {
    PullResponse.Builder response = PullResponse.newBuilder();
    JsonFormat.parser()
        .merge(
            api("POST", subscription + ":pull", "{\"maxMessages\":10,\"returnImmediately\":true}"),
            response);
    return response.getReceivedMessagesList();
  }

  private AckReplyConsumer consumer(String subscription, String ackId, List<String> queue) {
    return new AckReplyConsumer() {
      @Override
      public void ack() {
        if (queue != null) {
          queue.add(ackId);
        } else {
          try {
            int status = sendAck(subscription, ackId);
            assertTrue("Initial ack failed: HTTP " + status, status >= 200 && status < 300);
          } catch (Exception failure) {
            throw new IllegalStateException("Initial acknowledgement request failed", failure);
          }
        }
      }

      @Override
      public void nack() {
        throw new AssertionError("This test does not nack old deliveries");
      }
    };
  }

  private int sendAck(String subscription, String ackId) throws Exception {
    return request("POST", subscription + ":acknowledge", "{\"ackIds\":[\"" + ackId + "\"]}")
        .statusCode();
  }

  private HttpResponse<String> request(String method, String path, String body) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(base + path))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/json");
    if (credentials != null) {
      credentials.refreshIfExpired();
      request.header("Authorization", "Bearer " + credentials.getAccessToken().getTokenValue());
    }
    return http.send(
        request.method(method, HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private String api(String method, String path, String body) throws Exception {
    HttpResponse<String> response = request(method, path, body);
    // Never include response bodies: an error may repeat an acknowledgement token.
    assertTrue(
        method + " " + path + ": HTTP " + response.statusCode(),
        response.statusCode() >= 200 && response.statusCode() < 300);
    return response.body();
  }

  private void delete(String path) throws Exception {
    int status = request("DELETE", path, "").statusCode();
    assertTrue(
        "Cleanup " + path + ": HTTP " + status, status == 404 || (status >= 200 && status < 300));
  }
}

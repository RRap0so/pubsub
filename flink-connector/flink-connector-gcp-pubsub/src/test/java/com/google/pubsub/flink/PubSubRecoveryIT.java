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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Recovery scenarios with actual Flink jobs, checkpoints and managed operator state. */
public class PubSubRecoveryIT {
  private static MiniClusterWithClientResource cluster;
  private static final Map<String, List<String>> OUTPUT = new ConcurrentHashMap<>();
  private static final Map<String, String> COMPLETED = new ConcurrentHashMap<>();
  private static final Map<String, AtomicBoolean> FAIL_B_ONCE = new ConcurrentHashMap<>();
  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  private final HttpClient http = HttpClient.newHttpClient();
  private final List<JobClient> jobs = new ArrayList<>();
  private final List<String> snapshots = new ArrayList<>();
  private final List<String> runs = new ArrayList<>();
  private String endpoint;
  private String topic;
  private String subscription;
  private String suffix;

  @BeforeClass
  public static void startCluster() throws Exception {
    Assume.assumeNotNull(System.getenv("CHECKPOINT_TEST_EMULATOR"));
    Configuration config = new Configuration();
    config.setString("rest.bind-address", "127.0.0.1");
    config.setString("rest.address", "127.0.0.1");
    config.setString("rest.bind-port", "0");
    cluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(config)
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    cluster.before();
  }

  @AfterClass
  public static void stopCluster() {
    if (cluster != null) {
      cluster.after();
    }
  }

  @Before
  public void createSubscription() throws Exception {
    endpoint = System.getenv("CHECKPOINT_TEST_EMULATOR");
    assertTrue(endpoint.matches("127\\.0\\.0\\.1:[0-9]+"));
    String clientEndpoint = System.getenv("PUBSUB_EMULATOR_HOST");
    assertTrue(
        "PUBSUB_EMULATOR_HOST must match the test emulator",
        clientEndpoint == null || clientEndpoint.equals(endpoint));
    suffix = UUID.randomUUID().toString();
    topic = "projects/checkpoint-repro/topics/pubsub-" + suffix;
    subscription = "projects/checkpoint-repro/subscriptions/pubsub-" + suffix;
    api("PUT", topic, "{}");
    api("PUT", subscription, "{\"topic\":\"" + topic + "\",\"ackDeadlineSeconds\":10}");
  }

  @Test(timeout = 90000)
  public void rollbackToOldSavepoint_losesMessagesAcknowledgedByLaterCheckpoints()
      throws Exception {
    String firstRun = runId();
    JobClient original = startJob(firstRun, null, true);
    publish("A");
    await(() -> "A".equals(COMPLETED.get(firstRun)), "checkpoint containing A");
    String oldSavepoint =
        original
            .triggerSavepoint(savepointDir(), SavepointFormatType.CANONICAL)
            .get(20, TimeUnit.SECONDS);
    publish("B");
    await(() -> "A,B".equals(COMPLETED.get(firstRun)), "newer checkpoint containing B");
    cancel(original);

    String secondRun = runId();
    startJob(secondRun, oldSavepoint, true);
    publish("C");
    await(() -> OUTPUT.get(secondRun).contains("C|A,C"), "restored state with C");
    assertEquals(List.of("C|A,C"), OUTPUT.get(secondRun));
    // Desired full recovery is A,B,C. B is absent from both state and input after rollback.
  }

  @Test(timeout = 90000)
  public void taskFailure_recoversLatestCheckpointAndUnacknowledgedMessage() throws Exception {
    String run = runId();
    FAIL_B_ONCE.put(run, new AtomicBoolean(true));
    startJob(run, null, true);
    publish("A");
    await(() -> "A".equals(COMPLETED.get(run)), "checkpoint containing A");
    publish("B"); // Mapper deliberately fails once before changing B's managed state.
    await(() -> OUTPUT.get(run).contains("B|A,B"), "automatic recovery and B redelivery");
    assertEquals(false, FAIL_B_ONCE.get(run).get());
  }

  @Test(timeout = 90000)
  public void snapshotAndSavepoint_replayCanDoubleCountAlreadyCheckpointedMessage()
      throws Exception {
    String firstRun = runId();
    JobClient original = startJob(firstRun, null, false);
    publish("A");
    await(() -> OUTPUT.get(firstRun).contains("A|A"), "A processed before checkpoint");
    String snapshot = "projects/checkpoint-repro/snapshots/pubsub-" + suffix;
    api("PUT", snapshot, "{\"subscription\":\"" + subscription + "\"}");
    snapshots.add(snapshot);
    String savepoint =
        original
            .stopWithSavepoint(false, savepointDir(), SavepointFormatType.CANONICAL)
            .get(20, TimeUnit.SECONDS);
    jobs.remove(original);

    api("POST", subscription + ":seek", "{\"snapshot\":\"" + snapshot + "\"}");
    String secondRun = runId();
    startJob(secondRun, savepoint, true);
    await(() -> OUTPUT.get(secondRun).contains("A|A,A"), "A replayed into restored state");
    assertEquals(List.of("A|A,A"), OUTPUT.get(secondRun));
  }

  private JobClient startJob(String run, String savepoint, boolean checkpointing) throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "3");
    config.setString("restart-strategy.fixed-delay.delay", "100 ms");
    if (savepoint != null) {
      config.setString("execution.savepoint.path", savepoint);
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.setMaxParallelism(1);
    if (checkpointing) {
      env.enableCheckpointing(200);
    } else {
      // Source requires checkpointing enabled, but leave time for the controlled snapshot.
      env.enableCheckpointing(600000);
    }
    env.getCheckpointConfig().setCheckpointTimeout(20000);
    env.fromSource(
            PubSubSource.<String>builder()
                .setProjectName("checkpoint-repro")
                .setSubscriptionName("pubsub-" + suffix)
                .setEndpoint("emulator:///" + endpoint)
                .setDeserializationSchema(
                    PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .build(),
            WatermarkStrategy.noWatermarks(),
            "pubsub")
        .uid("pubsub")
        .map(new SessionAccumulator(run))
        .uid("session-state")
        .returns(Types.STRING)
        .sinkTo(new DiscardingSink<>())
        .uid("discard");
    JobClient job = env.executeAsync("pubsub-repro-" + run);
    jobs.add(job);
    return job;
  }

  public static class SessionAccumulator extends RichMapFunction<String, String>
      implements CheckpointedFunction, CheckpointListener {
    private final String run;
    private String session = "";
    private transient ListState<String> managed;
    private final Map<Long, String> checkpointSessions = new ConcurrentHashMap<>();

    public SessionAccumulator(String run) {
      this.run = run;
    }

    @Override
    public String map(String value) {
      AtomicBoolean fail = FAIL_B_ONCE.get(run);
      if (value.equals("B") && fail != null && fail.compareAndSet(true, false)) {
        throw new RuntimeException("Injected recovery failure before processing B");
      }
      session = session.isEmpty() ? value : session + "," + value;
      String output = value + "|" + session;
      OUTPUT.get(run).add(output);
      return output;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      managed =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("session", Types.STRING));
      for (String restored : managed.get()) {
        session = restored;
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      managed.clear();
      managed.add(session);
      checkpointSessions.put(context.getCheckpointId(), session);
    }

    @Override
    public void notifyCheckpointComplete(long id) {
      String state = checkpointSessions.get(id);
      if (state != null) {
        COMPLETED.put(run, state);
      }
      checkpointSessions.keySet().removeIf(checkpoint -> checkpoint <= id);
    }
  }

  private String runId() {
    String run = UUID.randomUUID().toString();
    runs.add(run);
    OUTPUT.put(run, new CopyOnWriteArrayList<>());
    return run;
  }

  private String savepointDir() throws Exception {
    return temporary.newFolder().toURI().toString();
  }

  private void cancel(JobClient job) throws Exception {
    job.cancel().get(20, TimeUnit.SECONDS);
    jobs.remove(job);
  }

  private void await(BooleanSupplier condition, String description) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(
        "Timed out: " + description + "; output=" + OUTPUT + "; checkpoints=" + COMPLETED,
        condition.getAsBoolean());
  }

  private void publish(String value) throws Exception {
    String data = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    api("POST", topic + ":publish", "{\"messages\":[{\"data\":\"" + data + "\"}]}");
  }

  private void api(String method, String path, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://" + endpoint + "/v1/" + path))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
    assertTrue(response.body(), response.statusCode() >= 200 && response.statusCode() < 300);
  }

  @After
  public void cleanUp() throws Exception {
    for (JobClient job : new ArrayList<>(jobs)) {
      cancel(job);
    }
    for (String snapshot : snapshots) {
      api("DELETE", snapshot, "");
    }
    if (subscription != null) {
      api("DELETE", subscription, "");
      api("DELETE", topic, "");
    }
    for (String run : runs) {
      OUTPUT.remove(run);
      COMPLETED.remove(run);
      FAIL_B_ONCE.remove(run);
    }
  }
}

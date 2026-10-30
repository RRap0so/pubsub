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

import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.ListSnapshotsResponse;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PullResponse;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.InMemoryReporter;
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
  private static InMemoryReporter metrics;
  private static final Map<String, List<String>> OUTPUT = new ConcurrentHashMap<>();
  private static final Map<String, String> COMPLETED = new ConcurrentHashMap<>();
  private static final Map<String, AtomicBoolean> FAIL_B_ONCE = new ConcurrentHashMap<>();
  private static final Map<String, AtomicInteger> CLOSED_TASKS = new ConcurrentHashMap<>();
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
    metrics = InMemoryReporter.create();
    metrics.addToConfiguration(config);
    cluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(config)
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .withHaLeadershipControl()
                .build());
    cluster.before();
  }

  @AfterClass
  public static void stopCluster() {
    try {
      if (cluster != null) {
        cluster.after();
      }
    } finally {
      if (metrics != null) {
        metrics.close();
      }
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
    assertSubscriptionDrained();

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
  public void replaySnapshots_restoreAcknowledgedInputFromOlderSavepoint() throws Exception {
    verifyReplayRecovery(1);
  }

  @Test(timeout = 90000)
  public void replaySnapshots_restoreAcknowledgedInputWithParallelReaders() throws Exception {
    verifyReplayRecovery(2);
  }

  private void verifyReplayRecovery(int parallelism) throws Exception {
    String firstRun = runId();
    JobClient original = startJob(firstRun, null, false, 2, parallelism);
    publish("A");
    await(
        () -> OUTPUT.get(firstRun).stream().anyMatch(value -> value.startsWith("A|")),
        "A processed");
    cluster
        .getClusterClient()
        .triggerCheckpoint(original.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    String oldSavepoint =
        original
            .triggerSavepoint(savepointDir(), SavepointFormatType.CANONICAL)
            .get(20, TimeUnit.SECONDS);
    publish("B");
    await(
        () -> OUTPUT.get(firstRun).stream().anyMatch(value -> value.startsWith("B|")),
        "B processed");
    cluster
        .getClusterClient()
        .triggerCheckpoint(original.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    cancel(original); // Flush acknowledgements: B must now require explicit replay.
    assertSubscriptionDrained();

    String secondRun = runId();
    startJob(secondRun, oldSavepoint, false, 2, parallelism);
    publish("C");
    await(
        () -> OUTPUT.get(secondRun).stream().anyMatch(value -> value.startsWith("B|")),
        "previously acknowledged B recovered from the older savepoint");
    await(
        () -> OUTPUT.get(secondRun).stream().anyMatch(value -> value.startsWith("C|")),
        "C processed");
    if (parallelism == 1) {
      String last = OUTPUT.get(secondRun).get(OUTPUT.get(secondRun).size() - 1);
      assertTrue(
          last,
          java.util.Arrays.asList(last.split("\\|", 2)[1].split(","))
              .containsAll(List.of("A", "B", "C")));
    }
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

  @Test(timeout = 90000)
  public void replayDeduplication_keepsRestoredBusinessStateFromDoubleCounting() throws Exception {
    verifyDeduplication(1);
  }

  @Test(timeout = 90000)
  public void replayDeduplication_keepsBusinessStateCorrectAfterRescaling() throws Exception {
    verifyDeduplication(2);
  }

  @Test(timeout = 90000)
  public void replayDeduplication_taskFailureRollsBackIdsTogetherWithBusinessState()
      throws Exception {
    String run = runId();
    JobClient job = startJob(run, null, false, 2, 2, true);
    publish("A");
    await(() -> OUTPUT.get(run).contains("A|A"), "A processed");
    cluster
        .getClusterClient()
        .triggerCheckpoint(job.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    FAIL_B_ONCE.put(run, new AtomicBoolean(true));
    publish("B");
    await(() -> OUTPUT.get(run).contains("B|A,B"), "failed B retried after ID state rollback");
    assertEquals(false, FAIL_B_ONCE.get(run).get());
    assertEquals(List.of("A|A", "B|A,B"), OUTPUT.get(run));
  }

  @Test(timeout = 90000)
  public void replayDeduplication_stopWithSavepointRestoresWithoutDraining() throws Exception {
    verifyStopWithSavepoint(false);
  }

  @Test(timeout = 90000)
  public void replayDeduplication_stopWithSavepointRestoresAfterDraining() throws Exception {
    verifyStopWithSavepoint(true);
  }

  private void verifyStopWithSavepoint(boolean drain) throws Exception {
    String firstRun = runId();
    JobClient original = startJob(firstRun, null, false, 2, 2, true);
    publish("A");
    await(() -> OUTPUT.get(firstRun).contains("A|A"), "A accepted before deployment stop");
    String savepoint =
        original
            .stopWithSavepoint(drain, savepointDir(), SavepointFormatType.CANONICAL)
            .get(20, TimeUnit.SECONDS);
    original.getJobExecutionResult().get(20, TimeUnit.SECONDS);
    jobs.remove(original);

    String secondRun = runId();
    JobClient restored = startJob(secondRun, savepoint, false, 2, 1, true);
    publish("C");
    await(() -> OUTPUT.get(secondRun).contains("C|A,C"), "new input after deployment restore");
    await(() -> duplicateDrops(restored) >= 1, "stopped job's A replayed and suppressed");
    cluster
        .getClusterClient()
        .triggerCheckpoint(restored.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    cancel(restored);
    assertSubscriptionDrained();
    assertEquals(List.of("C|A,C"), OUTPUT.get(secondRun));
  }

  @Test(timeout = 90000)
  public void replayDeduplication_jobMasterLeadershipLossRecoversCheckpointAndPendingInput()
      throws Exception {
    String run = runId();
    JobClient job = startJob(run, null, false, 2, 2, true);
    publish("A");
    await(() -> OUTPUT.get(run).contains("A|A"), "A processed before leadership loss");
    cluster
        .getClusterClient()
        .triggerCheckpoint(job.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    var leadership = cluster.getMiniCluster().getHaLeadershipControl().orElseThrow();
    // Revoke the JobMaster itself, rather than injecting another task failure. The embedded HA
    // service retains the completed checkpoint while the old JobMaster and its readers shut down.
    leadership.revokeJobMasterLeadership(job.getJobID()).get(20, TimeUnit.SECONDS);
    try {
      await(() -> CLOSED_TASKS.get(run).get() == 2, "old JobMaster's business tasks stopped");
      publish("B");
    } finally {
      leadership.grantJobMasterLeadership(job.getJobID()).get(20, TimeUnit.SECONDS);
    }
    await(() -> OUTPUT.get(run).contains("B|A,B"), "B processed after JobMaster re-election");
    // This can be snapshot replay or ordinary redelivery if the checkpoint completion notice
    // was lost. The older-savepoint tests independently prove recovery of acknowledged input.
    await(() -> duplicateDrops(job) >= 1, "A redelivered and suppressed after leadership recovery");
    cluster
        .getClusterClient()
        .triggerCheckpoint(job.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    cancel(job);
    assertSubscriptionDrained();
    assertEquals(List.of("A|A", "B|A,B"), OUTPUT.get(run));
  }

  private void verifyDeduplication(int restoredParallelism) throws Exception {
    String firstRun = runId();
    JobClient original = startJob(firstRun, null, false, 2, 1, true);
    publish("A");
    await(() -> OUTPUT.get(firstRun).contains("A|A"), "A accepted before snapshot");
    // A is represented in Flink state but is still unacknowledged in this provider snapshot.
    String savepoint =
        original
            .triggerSavepoint(savepointDir(), SavepointFormatType.CANONICAL)
            .get(20, TimeUnit.SECONDS);
    publish("B");
    await(() -> OUTPUT.get(firstRun).contains("B|A,B"), "B processed after the savepoint");
    cluster
        .getClusterClient()
        .triggerCheckpoint(original.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    cancel(original);
    assertSubscriptionDrained();

    String secondRun = runId();
    JobClient restored = startJob(secondRun, savepoint, false, 2, restoredParallelism, true);
    publish("C");
    await(() -> OUTPUT.get(secondRun).size() >= 2, "acknowledged B replayed alongside new C");
    await(() -> duplicateDrops(restored) >= 1, "replayed A observed and filtered by the ID index");
    cluster
        .getClusterClient()
        .triggerCheckpoint(restored.getJobID(), CheckpointType.FULL)
        .get(20, TimeUnit.SECONDS);
    cancel(restored);
    assertSubscriptionDrained();
    List<String> output = OUTPUT.get(secondRun);
    assertEquals(2, output.size());
    assertEquals(
        java.util.Set.of("B", "C"),
        output.stream()
            .map(value -> value.split("\\|", 2)[0])
            .collect(java.util.stream.Collectors.toSet()));
    String lastState = output.get(output.size() - 1).split("\\|", 2)[1];
    assertEquals(3, lastState.split(",").length);
    assertEquals(
        java.util.Set.of("A", "B", "C"),
        java.util.Set.copyOf(java.util.Arrays.asList(lastState.split(","))));
  }

  private long duplicateDrops(JobClient job) {
    return metrics.findMetrics(job.getJobID(), ".*pubsubReplayDuplicatesDropped").values().stream()
        .mapToLong(metric -> ((Counter) metric).getCount())
        .sum();
  }

  @Test(timeout = 120000)
  public void replayDeduplication_bulkPublishingAcrossSavepointAndRestore_preservesEveryRecord()
      throws Exception {
    int beforeSavepoint = 500;
    int beforeRollback = 1500;
    int total = 3000;
    Map<String, String> publications = new ConcurrentHashMap<>();
    ExecutorService publisher = Executors.newSingleThreadExecutor();
    CountDownLatch resumeDuringRestore = new CountDownLatch(1);
    try {
      String firstRun = runId();
      JobClient original = startJob(firstRun, null, false, 4, 2, true, 10000L);
      publishRange(0, beforeSavepoint, publications);
      await(() -> OUTPUT.get(firstRun).size() == beforeSavepoint, "initial 500 records processed");
      Future<?> publishing =
          publisher.submit(
              () -> {
                try {
                  for (int start = beforeSavepoint; start < total; start += 25) {
                    if (start == beforeRollback) {
                      assertTrue(
                          "Restore phase never started",
                          resumeDuringRestore.await(45, TimeUnit.SECONDS));
                    }
                    publishRange(start, start + 25, publications);
                    Thread.sleep(20);
                  }
                } catch (Exception failure) {
                  throw new RuntimeException("Synthetic publisher failed", failure);
                }
              });
      // Publication continues while the older savepoint is taken, so its precise membership
      // is deliberately unknown. The final assertion must recover all IDs regardless of it.
      String oldSavepoint =
          original
              .triggerSavepoint(savepointDir(), SavepointFormatType.CANONICAL)
              .get(20, TimeUnit.SECONDS);
      await(
          () -> OUTPUT.get(firstRun).size() == beforeRollback,
          "all 1500 pre-rollback records processed");
      cluster
          .getClusterClient()
          .triggerCheckpoint(original.getJobID(), CheckpointType.FULL)
          .get(20, TimeUnit.SECONDS);
      cancel(original); // Flush newer acknowledgements before rolling back to the old savepoint.
      assertSubscriptionDrained();

      String secondRun = runId();
      resumeDuringRestore.countDown(); // Publish the remaining 1500 while recovery starts.
      JobClient restored = startJob(secondRun, oldSavepoint, false, 4, 1, true, 10000L);
      publishing.get(30, TimeUnit.SECONDS); // Publisher errors must not masquerade as source loss.
      assertEquals(total, publications.size());
      assertEquals(
          "Every publication must have a distinct provider ID",
          total,
          Set.copyOf(publications.values()).size());
      long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
      while (System.nanoTime() < deadline) {
        List<String> output = OUTPUT.get(secondRun);
        if (!output.isEmpty()
            && output.get(output.size() - 1).split("\\|", 2)[1].split(",").length >= total) {
          break;
        }
        Thread.sleep(20);
      }
      cluster
          .getClusterClient()
          .triggerCheckpoint(restored.getJobID(), CheckpointType.FULL)
          .get(20, TimeUnit.SECONDS);
      long dropped = duplicateDrops(restored);
      cancel(restored);
      assertSubscriptionDrained();

      List<String> output = OUTPUT.get(secondRun);
      assertTrue("Restored job produced no records", !output.isEmpty());
      String[] state = output.get(output.size() - 1).split("\\|", 2)[1].split(",");
      Set<String> recovered = Set.copyOf(java.util.Arrays.asList(state));
      Set<String> missing = new java.util.HashSet<>(publications.keySet());
      missing.removeAll(recovered);
      System.out.println(
          "BULK_REPLAY published="
              + publications.size()
              + " businessCount="
              + state.length
              + " uniqueBusinessIds="
              + recovered.size()
              + " replayDuplicatesDropped="
              + dropped
              + " missing="
              + missing);
      assertEquals("Missing or unexpected business IDs", publications.keySet(), recovered);
      assertEquals("Replay double-counted business records", total, state.length);
      assertTrue("The replay must actually exercise deduplication", dropped > 0);
    } finally {
      resumeDuringRestore.countDown();
      publisher.shutdownNow();
      assertTrue("Publisher did not stop", publisher.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private void publishRange(int from, int to, Map<String, String> publications) throws Exception {
    for (int start = from; start < to; start += 25) {
      int end = Math.min(start + 25, to);
      List<String> messages = new ArrayList<>();
      for (int id = start; id < end; id++) {
        String value = "record-" + id;
        String data = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        messages.add("{\"data\":\"" + data + "\"}");
      }
      PublishResponse.Builder response = PublishResponse.newBuilder();
      JsonFormat.parser()
          .merge(
              api(
                  "POST",
                  topic + ":publish",
                  "{\"messages\":[" + String.join(",", messages) + "]}"),
              response);
      assertEquals(
          "Publish response must account for every submitted record",
          end - start,
          response.getMessageIdsCount());
      for (int id = start; id < end; id++) {
        publications.put("record-" + id, response.getMessageIds(id - start));
      }
    }
  }

  public static class KeyedSessionAccumulator extends RichMapFunction<String, String> {
    private final String run;
    private transient ValueState<String> session;

    public KeyedSessionAccumulator(String run) {
      this.run = run;
    }

    @Override
    public void open(OpenContext context) throws Exception {
      session = getRuntimeContext().getState(new ValueStateDescriptor<>("session", Types.STRING));
    }

    @Override
    public void close() {
      CLOSED_TASKS.get(run).incrementAndGet();
    }

    @Override
    public String map(String value) throws Exception {
      AtomicBoolean fail = FAIL_B_ONCE.get(run);
      if (value.equals("B") && fail != null && fail.compareAndSet(true, false)) {
        throw new RuntimeException("Injected recovery failure before processing B");
      }
      String previous = session.value();
      String next = previous == null ? value : previous + "," + value;
      session.update(next);
      String result = value + "|" + next;
      OUTPUT.get(run).add(result);
      return result;
    }
  }

  private JobClient startJob(String run, String savepoint, boolean checkpointing) throws Exception {
    return startJob(run, savepoint, checkpointing, 0, 1);
  }

  private JobClient startJob(
      String run, String savepoint, boolean checkpointing, int replayRetention, int parallelism)
      throws Exception {
    return startJob(run, savepoint, checkpointing, replayRetention, parallelism, false);
  }

  private JobClient startJob(
      String run,
      String savepoint,
      boolean checkpointing,
      int replayRetention,
      int parallelism,
      boolean deduplicate)
      throws Exception {
    return startJob(
        run, savepoint, checkpointing, replayRetention, parallelism, deduplicate, 1000L);
  }

  private JobClient startJob(
      String run,
      String savepoint,
      boolean checkpointing,
      int replayRetention,
      int parallelism,
      boolean deduplicate,
      long maxOutstandingMessages)
      throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "3");
    config.setString("restart-strategy.fixed-delay.delay", "100 ms");
    if (savepoint != null) {
      config.setString("execution.savepoint.path", savepoint);
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(parallelism);
    env.setMaxParallelism(16);
    if (checkpointing) {
      env.enableCheckpointing(200);
    } else {
      // Source requires checkpointing enabled, but leave time for the controlled snapshot.
      env.enableCheckpointing(600000);
    }
    env.getCheckpointConfig().setCheckpointTimeout(20000);
    PubSubSource.Builder<String> source =
        PubSubSource.<String>builder()
            .setProjectName("checkpoint-repro")
            .setSubscriptionName("pubsub-" + suffix)
            .setEndpoint("emulator:///" + endpoint)
            .setMaxOutstandingMessagesCount(maxOutstandingMessages)
            .setDeserializationSchema(
                PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()));
    if (replayRetention > 0) {
      source.setReplaySnapshotRetentionCount(replayRetention);
    }
    if (deduplicate) {
      PubSubReplayDeduplication.read(
              env, source.build(), WatermarkStrategy.noWatermarks(), "pubsub", Duration.ofDays(8))
          .keyBy(value -> "session")
          .map(new KeyedSessionAccumulator(run))
          .uid("session-state")
          .returns(Types.STRING)
          .sinkTo(new DiscardingSink<>())
          .uid("discard");
    } else {
      env.fromSource(source.build(), WatermarkStrategy.noWatermarks(), "pubsub")
          .uid("pubsub")
          .map(new SessionAccumulator(run))
          .uid("session-state")
          .returns(Types.STRING)
          .sinkTo(new DiscardingSink<>())
          .uid("discard");
    }
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
    CLOSED_TASKS.put(run, new AtomicInteger());
    return run;
  }

  private String savepointDir() throws Exception {
    return temporary.newFolder().toURI().toString();
  }

  private void cancel(JobClient job) throws Exception {
    job.cancel().get(20, TimeUnit.SECONDS);
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    boolean terminated = false;
    while (System.nanoTime() < deadline) {
      if (job.getJobStatus().get(5, TimeUnit.SECONDS).isGloballyTerminalState()) {
        terminated = true;
        break;
      }
      Thread.sleep(20);
    }
    assertTrue("Old job must terminate before subscription replay", terminated);
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

  private void assertSubscriptionDrained() throws Exception {
    PullResponse.Builder response = PullResponse.newBuilder();
    JsonFormat.parser()
        .merge(
            api("POST", subscription + ":pull", "{\"maxMessages\":100,\"returnImmediately\":true}"),
            response);
    assertEquals(
        "Input must already be acknowledged before exercising replay",
        List.of(),
        response.getReceivedMessagesList());
  }

  private String api(String method, String path, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://" + endpoint + "/v1/" + path))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
    assertTrue(response.body(), response.statusCode() >= 200 && response.statusCode() < 300);
    return response.body();
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
      var owned = ListSnapshotsResponse.newBuilder();
      JsonFormat.parser().merge(api("GET", "projects/checkpoint-repro/snapshots", ""), owned);
      for (var snapshot : owned.getSnapshotsList()) {
        if (snapshot.getTopic().equals(topic)) {
          api("DELETE", snapshot.getName(), "");
        }
      }
      api("DELETE", subscription, "");
      api("DELETE", topic, "");
    }
    for (String run : runs) {
      OUTPUT.remove(run);
      COMPLETED.remove(run);
      FAIL_B_ONCE.remove(run);
      CLOSED_TASKS.remove(run);
    }
  }
}

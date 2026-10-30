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
package com.google.pubsub.flink;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.pubsub.flink.internal.source.enumerator.PubSubCheckpointSerializer;
import com.google.pubsub.flink.internal.source.enumerator.PubSubReplaySnapshots;
import com.google.pubsub.flink.internal.source.enumerator.PubSubSnapshotStore;
import com.google.pubsub.flink.internal.source.enumerator.PubSubSplitEnumerator;
import com.google.pubsub.flink.internal.source.reader.AckTracker;
import com.google.pubsub.flink.internal.source.reader.PubSubAckTracker;
import com.google.pubsub.flink.internal.source.reader.PubSubNotifyingPullSubscriber;
import com.google.pubsub.flink.internal.source.reader.PubSubReaderMetrics;
import com.google.pubsub.flink.internal.source.reader.PubSubSourceReader;
import com.google.pubsub.flink.internal.source.reader.PubSubSplitReader;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplitSerializer;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import com.google.pubsub.flink.util.EmulatorEndpoint;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.UUID;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.threeten.bp.Duration;

/**
 * Google Cloud Pub/Sub source to pull messages from a Pub/Sub subscription.
 *
 * <p>{@link PubSubSource} is constructed and configured using {@link Builder}. {@link PubSubSource}
 * cannot be configured after it is built. See {@link Builder} for how {@link PubSubSource} can be
 * configured.
 */
@AutoValue
public abstract class PubSubSource<OutputT>
    implements Source<OutputT, SubscriptionSplit, PubSubEnumeratorCheckpoint>,
        ResultTypeQueryable<OutputT> {
  public abstract String projectName();

  public abstract String subscriptionName();

  public abstract PubSubDeserializationSchema<OutputT> deserializationSchema();

  public abstract Optional<Long> maxOutstandingMessagesCount();

  public abstract Optional<Long> maxOutstandingMessagesBytes();

  public abstract Optional<Integer> parallelPullCount();

  public abstract Optional<Credentials> credentials();

  public abstract Optional<String> endpoint();

  public abstract Optional<Integer> replaySnapshotRetentionCount();

  abstract String replaySnapshotNamespace();

  abstract boolean replayDeduplicationRequired();

  public static <OutputT> Builder<OutputT> builder() {
    return new AutoValue_PubSubSource.Builder<OutputT>()
        .setReplaySnapshotNamespace(UUID.randomUUID().toString().replace("-", ""))
        .setReplayDeduplicationRequired(false);
  }

  Subscriber createSubscriber(MessageReceiver receiver) {
    return createSubscriber(receiver, new PubSubReaderMetrics(new UnregisteredMetricsGroup()));
  }

  Subscriber createSubscriber(MessageReceiver receiver, PubSubReaderMetrics metrics) {
    MessageReceiverWithAckResponse observedReceiver =
        (message, consumer) -> receiver.receiveMessage(message, metrics.observe(consumer));
    Subscriber.Builder builder =
        Subscriber.newBuilder(
            ProjectSubscriptionName.of(projectName(), subscriptionName()).toString(),
            observedReceiver);
    builder.setFlowControlSettings(
        FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(maxOutstandingMessagesCount().or(1000L))
            .setMaxOutstandingRequestBytes(
                maxOutstandingMessagesBytes().or(100L * 1024L * 1024L)) // 100MB
            .build());
    if (parallelPullCount().isPresent()) {
      builder.setParallelPullCount(parallelPullCount().get());
    }
    if (credentials().isPresent()) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials().get()));
    }
    if (endpoint().isPresent()) {
      builder.setEndpoint(endpoint().get());
    }

    String emulatorEndpoint = EmulatorEndpoint.getEmulatorEndpoint(endpoint());
    if (emulatorEndpoint != null) {
      builder.setCredentialsProvider(NoCredentialsProvider.create());
      builder.setEndpoint(emulatorEndpoint);
    }
    builder.setChannelProvider(createChannelProvider(emulatorEndpoint));
    return builder.build();
  }

  InstantiatingGrpcChannelProvider createChannelProvider(String emulatorEndpoint) {
    // Let the SDK create and own channels so stop/failure also closes emulator transports.
    InstantiatingGrpcChannelProvider.Builder provider =
        SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
            .setMaxInboundMessageSize(20 * 1024 * 1024)
            .setMaxInboundMetadataSize(4 * 1024 * 1024)
            .setKeepAliveTime(Duration.ofMinutes(5))
            .setHeaderProvider(
                FixedHeaderProvider.create(
                    "x-goog-api-client", "PubSub-Flink-Connector/1.0.1-SNAPSHOT"));
    if (emulatorEndpoint != null) {
      provider
          .setEndpoint(emulatorEndpoint)
          .setChannelConfigurator(channel -> channel.usePlaintext());
    }
    return provider.build();
  }

  private PubSubSplitReader createSplitReader(AckTracker ackTracker, PubSubReaderMetrics metrics) {
    return new PubSubSplitReader(
        () ->
            new PubSubNotifyingPullSubscriber(
                receiver -> createSubscriber(receiver, metrics), ackTracker));
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<OutputT, SubscriptionSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    PubSubDeserializationSchema<OutputT> schema = deserializationSchema();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return readerContext.metricGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
          }
        });
    MetricGroup metrics = readerContext.metricGroup().addGroup("pubsub");
    PubSubReaderMetrics acknowledgementMetrics = new PubSubReaderMetrics(metrics);
    return new PubSubSourceReader<>(
        schema,
        new PubSubAckTracker(metrics),
        ackTracker -> createSplitReader(ackTracker, acknowledgementMetrics),
        new Configuration(),
        readerContext);
  }

  @Override
  public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> createEnumerator(
      SplitEnumeratorContext<SubscriptionSplit> enumContext) {
    return new PubSubSplitEnumerator(
        ProjectSubscriptionName.of(projectName(), subscriptionName()),
        enumContext,
        new HashMap<Integer, SubscriptionSplit>(),
        createReplaySnapshots(null),
        replayDeduplicationRequired());
  }

  @Override
  public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<SubscriptionSplit> enumContext,
      PubSubEnumeratorCheckpoint checkpoint) {
    Preconditions.checkState(
        checkpoint.getReplayDeduplicationRequired() == replayDeduplicationRequired(),
        "Replay deduplication cannot be enabled or disabled when restoring an existing checkpoint");
    Preconditions.checkState(
        replaySnapshotRetentionCount().isPresent()
            || checkpoint.getReplaySnapshotNamespace().isEmpty(),
        "Enable replay snapshot recovery to restore this checkpoint");
    Preconditions.checkState(
        !replaySnapshotRetentionCount().isPresent()
            || !checkpoint.getReplaySnapshotNamespace().isEmpty(),
        "This checkpoint has no replay snapshot; acknowledged input cannot be recovered safely");
    Preconditions.checkArgument(
        checkpoint.getReplaySnapshotNamespace().isEmpty()
            || checkpoint
                .getReplaySubscription()
                .equals(ProjectSubscriptionName.of(projectName(), subscriptionName()).toString()),
        "Replay checkpoint belongs to a different subscription");
    HashMap<Integer, SubscriptionSplit> assignments = new HashMap<>();
    for (PubSubEnumeratorCheckpoint.Assignment assignment : checkpoint.getAssignmentsList()) {
      assignments.put(assignment.getSubtask(), SubscriptionSplit.fromProto(assignment.getSplit()));
    }
    return new PubSubSplitEnumerator(
        ProjectSubscriptionName.of(projectName(), subscriptionName()),
        enumContext,
        assignments,
        createReplaySnapshots(checkpoint),
        replayDeduplicationRequired());
  }

  private PubSubReplaySnapshots createReplaySnapshots(PubSubEnumeratorCheckpoint checkpoint) {
    if (!replaySnapshotRetentionCount().isPresent()) {
      return null;
    }
    PubSubSnapshotStore store;
    try {
      store = new PubSubSnapshotStore(projectName(), subscriptionName(), credentials(), endpoint());
    } catch (IOException failure) {
      throw new UncheckedIOException("Unable to open the replay snapshot client", failure);
    }
    try {
      return checkpoint == null
          ? PubSubReplaySnapshots.startFresh(
              store, replaySnapshotRetentionCount().get(), replaySnapshotNamespace())
          : PubSubReplaySnapshots.restore(store, replaySnapshotRetentionCount().get(), checkpoint);
    } catch (RuntimeException failure) {
      store.close();
      throw failure;
    }
  }

  @Override
  public SimpleVersionedSerializer<SubscriptionSplit> getSplitSerializer() {
    return new SubscriptionSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<PubSubEnumeratorCheckpoint> getEnumeratorCheckpointSerializer() {
    return new PubSubCheckpointSerializer();
  }

  @Override
  public TypeInformation<OutputT> getProducedType() {
    return deserializationSchema().getProducedType();
  }

  /** Builder to construct {@link PubSubSource}. */
  @AutoValue.Builder
  public abstract static class Builder<OutputT> {
    /**
     * Sets the GCP project ID that owns the subscription from which messages are pulled.
     *
     * <p>Setting this option is required to build {@link PubSubSource}.
     */
    public abstract Builder<OutputT> setProjectName(String projectName);

    /**
     * Sets the Pub/Sub subscription to which messages are pulled.
     *
     * <p>Setting this option is required to build {@link PubSubSource}.
     */
    public abstract Builder<OutputT> setSubscriptionName(String subscriptionName);

    /**
     * Sets the deserialization schema used to deserialize {@link PubsubMessage} for processing.
     *
     * <p>Setting this option is required to build {@link PubSubSource}.
     */
    public abstract Builder<OutputT> setDeserializationSchema(
        PubSubDeserializationSchema<OutputT> deserializationSchema);

    /**
     * Sets the max number of messages that can be outstanding to a StreamingPull connection.
     *
     * <p>Defaults to 1,000 outstanding messages. A message is considered outstanding when it is
     * delivered and waiting to be acknowledged in the next successful checkpoint. Google Cloud
     * Pub/Sub suspends message delivery to StreamingPull connections that reach this limit.
     *
     * <p>If set, this value must be > 0. Otherwise, calling {@link build} will throw an exception.
     */
    public abstract Builder<OutputT> setMaxOutstandingMessagesCount(Long count);

    /**
     * Sets the max cumulative message bytes that can be outstanding to a StreamingPull connection.
     *
     * <p>Defaults to 100 MB. A message is considered outstanding when it is delivered and waiting
     * to be acknowledged in the next successful checkpoint. Google Cloud Pub/Sub suspends message
     * delivery to StreamingPull connections that reach this limit.
     *
     * <p>If set, this value must be > 0. Otherwise, calling {@link build} will throw an exception.
     */
    public abstract Builder<OutputT> setMaxOutstandingMessagesBytes(Long bytes);

    /**
     * Sets the number of StreamingPull connections opened by each {@link PubSubSource} subtask.
     *
     * <p>Defaults to 1.
     *
     * <p>If set, this value must be > 0. Otherwise, calling {@link build} will throw an exception.
     */
    public abstract Builder<OutputT> setParallelPullCount(Integer parallelPullCount);

    /**
     * Sets the credentials used when pulling messages from Google Cloud Pub/Sub.
     *
     * <p>If not set, then Application Default Credentials are used for authentication.
     */
    public abstract Builder<OutputT> setCredentials(Credentials credentials);

    /**
     * Sets the Google Cloud Pub/Sub service endpoint from which messages are pulled.
     *
     * <p>Defaults to connecting to the global endpoint, which routes requests to the nearest
     * regional endpoint.
     */
    public abstract Builder<OutputT> setEndpoint(String endpoint);

    /**
     * Enables checkpoint-coordinated Pub/Sub snapshots and retains the last {@code count} snapshot
     * boundaries at or before the newest completed checkpoint, plus in-flight boundaries. Total
     * tracked boundaries are capped at {@code count + 1}; use one concurrent checkpoint.
     *
     * <p>Disabled by default. Requires a dedicated subscription and snapshot create, delete, list,
     * and seek permissions. Snapshot storage is billed separately. Older or expired snapshots
     * cannot be restored; recovery fails rather than silently skipping acknowledged input.
     *
     * <p>Replay provides at-least-once input recovery and can duplicate records already in the
     * restored state. Snapshots remain after job shutdown for recovery and expire under Pub/Sub's
     * retention policy. Cleanup of abandoned jobs must be managed separately.
     */
    public abstract Builder<OutputT> setReplaySnapshotRetentionCount(Integer count);

    abstract Builder<OutputT> setReplaySnapshotNamespace(String namespace);

    abstract Builder<OutputT> setReplayDeduplicationRequired(boolean required);

    abstract PubSubSource<OutputT> autoBuild();

    public final PubSubSource<OutputT> build() {
      PubSubSource<OutputT> source = autoBuild();
      Preconditions.checkArgument(
          source.maxOutstandingMessagesCount().or(1L) > 0,
          "maxOutstandingMessagesCount, if set, must be a value greater than 0.");
      Preconditions.checkArgument(
          source.maxOutstandingMessagesBytes().or(1L) > 0,
          "maxOutstandingMessagesBytes, if set, must be a value greater than 0.");
      Preconditions.checkArgument(
          source.parallelPullCount().or(1) > 0,
          "parallelPullCount, if set, must be a value greater than 0.");
      Preconditions.checkArgument(
          source.replaySnapshotRetentionCount().or(1) > 0,
          "replaySnapshotRetentionCount, if set, must be a value greater than 0.");
      return source;
    }
  }
}

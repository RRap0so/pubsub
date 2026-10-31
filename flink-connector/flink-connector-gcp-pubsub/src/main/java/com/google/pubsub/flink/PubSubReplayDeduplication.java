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

import com.google.common.base.Preconditions;
import com.google.pubsub.v1.PubsubMessage;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

/** Builds a Pub/Sub stream with checkpointed, message-ID based replay suppression. */
public final class PubSubReplayDeduplication {
  private PubSubReplayDeduplication() {}

  /**
   * Reads replay-protected input through a keyed message-ID index before deserialization.
   *
   * <p>Requires replay snapshots and exactly-once checkpoint consistency. Keep the UID prefix and
   * maximum parallelism stable across restores, including rescaling. Existing checkpoints made
   * without this stage cannot be migrated: they contain no message-ID index.
   *
   * <p>Retention is processing time since first acceptance, including downtime, and must be at
   * least eight days. Expired IDs can be emitted again. Retention must cover the age of every
   * message already represented in restored state that a snapshot might replay, including delayed
   * acknowledgements. State grows with distinct message IDs within that period; size the state
   * backend and checkpoint storage accordingly. This does not make external side effects
   * transactional or deduplicate separate publications of the same payload.
   *
   * <p>This topology shuffles by message ID and does not preserve Pub/Sub ordering-key order across
   * parallel subtasks. Watermarks are assigned after the shuffle and deserialization; idleness
   * applies to that operator, not individual source splits, and source watermark alignment is not
   * supported by this helper.
   */
  public static <T> SingleOutputStreamOperator<T> read(
      StreamExecutionEnvironment env,
      PubSubSource<T> source,
      WatermarkStrategy<T> watermarks,
      String uidPrefix,
      Duration retention) {
    Preconditions.checkArgument(
        env.getCheckpointConfig().isCheckpointingEnabled(),
        "Replay deduplication requires checkpointing");
    Preconditions.checkArgument(
        env.getCheckpointConfig().getCheckpointingConsistencyMode()
            == CheckpointingMode.EXACTLY_ONCE,
        "Replay deduplication requires exactly-once checkpoint consistency");
    Preconditions.checkArgument(
        source.replaySnapshotRetentionCount().isPresent(),
        "Replay deduplication requires replay snapshots");
    Preconditions.checkArgument(
        uidPrefix != null && !uidPrefix.trim().isEmpty(), "A stable UID prefix is required");
    Preconditions.checkArgument(
        retention != null && retention.compareTo(Duration.ofDays(8)) >= 0,
        "Message-ID retention must be at least eight days");
    return env.fromSource(
            rawSource(source), WatermarkStrategy.noWatermarks(), "Pub/Sub replay input")
        .uid(uidPrefix)
        .keyBy(value -> value.f0, Types.STRING)
        .process(new Deduplicate(retention))
        .uid(uidPrefix + "-deduplicate")
        .map(new Decode<>(source.deserializationSchema()))
        .returns(source.getProducedType())
        .uid(uidPrefix + "-deserialize")
        .assignTimestampsAndWatermarks(watermarks)
        .uid(uidPrefix + "-watermarks");
  }

  static PubSubSource<Tuple2<String, byte[]>> rawSource(PubSubSource<?> source) {
    PubSubSource.Builder<Tuple2<String, byte[]>> builder =
        PubSubSource.<Tuple2<String, byte[]>>builder()
            .setProjectName(source.projectName())
            .setSubscriptionName(source.subscriptionName())
            .setDeserializationSchema(new EnvelopeSchema())
            .setReplaySnapshotNamespace(source.replaySnapshotNamespace())
            .setReplayDeduplicationRequired(true);
    source
        .maxOutstandingMessagesCount()
        .toJavaUtil()
        .ifPresent(builder::setMaxOutstandingMessagesCount);
    source
        .maxOutstandingMessagesBytes()
        .toJavaUtil()
        .ifPresent(builder::setMaxOutstandingMessagesBytes);
    source.parallelPullCount().toJavaUtil().ifPresent(builder::setParallelPullCount);
    source.credentials().toJavaUtil().ifPresent(builder::setCredentials);
    source.endpoint().toJavaUtil().ifPresent(builder::setEndpoint);
    source
        .replaySnapshotRetentionCount()
        .toJavaUtil()
        .ifPresent(builder::setReplaySnapshotRetentionCount);
    return builder.build();
  }

  static class EnvelopeSchema implements PubSubDeserializationSchema<Tuple2<String, byte[]>> {
    @Override
    public void open(DeserializationSchema.InitializationContext context) {}

    @Override
    public Tuple2<String, byte[]> deserialize(PubsubMessage message) {
      Preconditions.checkArgument(
          !message.getMessageId().isEmpty(), "Pub/Sub message ID is missing");
      return Tuple2.of(message.getMessageId(), message.toByteArray());
    }

    @Override
    public TypeInformation<Tuple2<String, byte[]>> getProducedType() {
      return new TupleTypeInfo<>(Types.STRING, Types.PRIMITIVE_ARRAY(Types.BYTE));
    }
  }

  static class Deduplicate
      extends KeyedProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>> {
    private final Duration retention;
    private transient ValueState<Boolean> seen;
    private transient Counter duplicatesDropped;

    Deduplicate(Duration retention) {
      this.retention = retention;
    }

    @Override
    public void open(OpenContext context) throws Exception {
      ValueStateDescriptor<Boolean> descriptor =
          new ValueStateDescriptor<>("pubsub-seen", Types.BOOLEAN);
      descriptor.enableTimeToLive(
          StateTtlConfig.newBuilder(retention)
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .build());
      seen = getRuntimeContext().getState(descriptor);
      duplicatesDropped =
          getRuntimeContext().getMetricGroup().counter("pubsubReplayDuplicatesDropped");
    }

    @Override
    public void processElement(
        Tuple2<String, byte[]> value, Context context, Collector<Tuple2<String, byte[]>> out)
        throws Exception {
      if (seen.value() == null) {
        seen.update(true);
        out.collect(value);
      } else {
        duplicatesDropped.inc();
      }
    }
  }

  static class Decode<T> extends RichMapFunction<Tuple2<String, byte[]>, T> {
    private final PubSubDeserializationSchema<T> schema;

    Decode(PubSubDeserializationSchema<T> schema) {
      this.schema = schema;
    }

    @Override
    public void open(OpenContext context) throws Exception {
      schema.open(
          new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
              return getRuntimeContext().getMetricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
              return SimpleUserCodeClassLoader.create(getRuntimeContext().getUserCodeClassLoader());
            }
          });
    }

    @Override
    public T map(Tuple2<String, byte[]> value) throws Exception {
      return schema.deserialize(PubsubMessage.parseFrom(value.f1));
    }
  }
}

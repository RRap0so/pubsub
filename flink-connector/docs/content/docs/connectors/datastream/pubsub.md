# Google Cloud Pub/Sub Connector

This connector provides access to reading data from and writing data to
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub).

## Usage

This library is currently not published to any repositories. Usage requires
building it from source and packaging it with your Flink application.

Prerequisites:

*   Unix-like environment (we use Linux, Mac OS X)
*   Git
*   Maven (we recommend version 3.8.6)
*   Java 17

```sh
git clone https://github.com/GoogleCloudPlatform/pubsub.git
cd pubsub/flink-connector
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective
module.

Flink applications built with Maven can include the connector as a dependency in
their pom.xml file by adding:

```xml
<dependency>
  <groupId>com.google.cloud.pubsub.flink</groupId>
  <artifactId>flink-connector-gcp-pubsub</artifactId>
  <version>1.0.0</version>
</dependency>
```

An example Flink application can be found under
[`flink-connector/flink-examples-gcp-pubsub/`](https://github.com/GoogleCloudPlatform/pubsub/tree/master/flink-connector/flink-examples-gcp-pubsub).

Learn more about Flink connector packaging
[here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/).

## Configuring Access to Google Cloud Pub/Sub

Requests sent to Google Cloud Pub/Sub must be
[authenticated](https://cloud.google.com/pubsub/docs/authentication). By
default, the connector library authenticates requests using
[Application Default Credentials](https://cloud.google.com/docs/authentication#adc).

Credentials can also be in the source and sink builders. The connector library
prioritizes using credentials set in builders over Application Default
Credentials. The snippet below shows how to authenticate using an OAuth 2.0
token.

```java
final String tokenValue = "...";

// Authenticate with OAuth 2.0 token when pulling messages from a subscription.
PubSubSource.<String>builder().setCredentials(
    GoogleCredentials.create(new AccessToken(tokenValue, /* expirationTime= */ null)))

// Authenticate with OAuth 2.0 token when publishing messages to a topic.
PubSubSink.<String>builder().setCredentials(
    GoogleCredentials.create(new AccessToken(tokenValue, /* expirationTime= */ null)))
```

The authenticating principal must be
[authorized](https://cloud.google.com/pubsub/docs/access-control) to pull
messages from a subscription when using Pub/Sub source or publish messages to a
topic when using Pub/Sub sink. Authorization is managed through Google
[IAM](https://cloud.google.com/security/products/iam) and can be configured
either at the Google Cloud project-level or the Pub/Sub resource-level.

## Pub/Sub Source

Pub/Sub source streams data from a single Google Cloud Pub/Sub subscription with
an at-least-once guarantee. The sample below shows the minimal configurations
required to build Pub/Sub source.

```java
PubSubSource.<String>builder()
    .setDeserializationSchema(
        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
    .setProjectName("my-project-name")
    .setSubscriptionName("my-subscription-name")
    .build()
```

### Subscription

Pub/Sub source only supports streaming messages from
[pull subscriptions](https://cloud.google.com/pubsub/docs/pull). Push
subscriptions are not supported.

Pub/Sub source can create parallel readers to to the same subscription. Since
Google Cloud Pub/Sub has no notion of subscription partitions or splits, a
message can be received by any reader. Google Cloud Pub/Sub automatically load
balances message delivery across readers.

### Deserialization Schema

`PubSubDeserializationSchema<T>` is required to define how
[`PubsubMessage`](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
is deserialized into an output that is suitable for processing.

For convenience,
`PubSubDeserializationSchema.dataOnly(DeserializationSchema<T> schema)` can be
used if the field `PubsubMessage.data` stores all of the data to be processed.
Additionally, Flink provides basic schemas for further convenience.

```java
// Deserialization output is PubsubMessage.data field converted to String type.
PubSubSource.<String>builder()
    .setDeserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
```

Implementing `PubSubDeserializationSchema<T>` is required to process data stored
in fields other than `PubsubMessage.data`.
<!-- TODO(matt-kwong): Add custom deserialization code example. -->

### Boundedness

Pub/Sub source streams unbounded data, only stopping when a Flink job stops or
fails.

### Checkpointing

Checkpointing is required to use Pub/Sub source. When a checkpoint completes,
all messages delivered before the last successful checkpoint are acknowledged to
Google Cloud Pub/Sub. In case of failure, messages delivered after the last
successful checkpoint are unacknowledged and will automatically be redelivered.
By default, source checkpoints contain subscription assignments, not a rewindable
input position. Restoring an older checkpoint does not rewind acknowledgements
made by newer checkpoints, so input missing from the restored state can be lost.

For at-least-once replay from retained checkpoints and savepoints, explicitly set
`PubSubSource.<OutputT>builder().setReplaySnapshotRetentionCount(count)` with a
positive count. The coordinator captures a Pub/Sub snapshot before the readers'
checkpoint barriers and stores its reference in checkpoint state. Restore seeks
to that snapshot before a coordinator readiness event permits restored readers to start pulling. Missing or expired snapshots fail
recovery rather than silently skipping input.

This option is disabled by default and requires a dedicated subscription and
snapshot create, get, list, delete, and seek permissions. Checkpoints written by this
version use a newer enumerator state format, so older connector versions cannot restore
them; downgrading the connector requires starting from fresh source state. It retains the last
`count` completed provider boundaries plus one shared pending boundary. Savepoints
without completion notifications can share an earlier boundary.

Restore preserves unreferenced snapshots because they may belong to newer completed
savepoints. Those resources remain until provider expiry.
They and pending deletions count toward the namespace's `count + 1` resource budget.
If that budget is full, a checkpoint reuses a valid known boundary rather than creating
another resource. Restarts therefore cannot evade the resource budget. The coordinator
lists the namespace once on startup or restore, then tracks its own creates and deletes
locally; checkpoint allocation does not repeat a project-wide list. External removals,
including provider expiry of unknown snapshots, require restarting the coordinator
to refresh that inventory.

After restoring an older checkpoint with a full budget, successive checkpoints may keep
selecting its older provider boundary. This increases replay and lets that boundary age.
A healthy running job can create a fresh boundary when expiry and successful deletion
free a slot, but recovery from a checkpoint referencing an expired boundary fails.

Failed deletes are queued, logged, and retried without failing checkpoint completion.
The queue is included in subsequent checkpoint state and retried after a successful
restore seek. This covers cleanup recorded in a completed checkpoint. A remote create
followed by a crash before its cleanup intent reaches a completed checkpoint can still
leave an unknown snapshot. Restore preserves it because it cannot distinguish it from
a valid newer savepoint. It counts toward the budget until a restart refreshes the
inventory after provider expiry.
If creating a new snapshot fails, an existing unexpired boundary can be reused to
allow checkpoint ACKs and backlog drainage. Recovery may consequently replay more
input. A missing or expired boundary is never used as a fallback. If no safe boundary
exists (including the first checkpoint of an old backlog), checkpointing fails rather
than claiming replay protection that was never established.

Older checkpoints outside the retained window cannot be restored. Pub/Sub bills retained
snapshot data separately. Replay is at least once and may duplicate records represented
in Flink state; the opt-in helper below suppresses provider-message-ID duplicates.

### Checkpointed replay deduplication

`PubSubReplayDeduplication.read` adds a keyed, checkpointed message-ID index before
user deserialization. For example:

```java
env.enableCheckpointing(10_000);
env.setMaxParallelism(128); // Keep this value stable across restores and rescaling.
PubSubSource<String> source = PubSubSource.<String>builder()
    .setProjectName("my-project")
    .setSubscriptionName("my-subscription")
    .setDeserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
    .setReplaySnapshotRetentionCount(4)
    .build();
DataStream<String> input = PubSubReplayDeduplication.read(
    env, source, WatermarkStrategy.noWatermarks(), "stable-pubsub-uid", Duration.ofDays(8));
```

Replay snapshots and exactly-once checkpoint consistency are required. Keep the UID
prefix, operator topology, and maximum parallelism stable. Existing checkpoints without
this stage cannot be migrated: they do not contain the message-ID index. Enabling or
disabling deduplication when restoring existing state is rejected.

Retention must be at least eight days and cover every message a retained snapshot
might replay. TTL uses processing time since first acceptance, including downtime;
duplicates do not extend it. Expired IDs can be emitted again. State grows with the
number of distinct provider IDs during that period, so size the state backend and
checkpoint storage accordingly. Separate publications of the same payload have
different IDs. External writes still require transactions or downstream idempotency.

**Ordering and event time:** this helper shuffles by provider message ID. With parallelism
above one, messages sharing a Pub/Sub ordering key can reach different subtasks, so
it does not preserve ordering-key order. The supplied watermark strategy runs after
that shuffle and deserialization, rather than on the source. Its idleness therefore
applies to the downstream watermark operator; source-split idleness and source watermark
alignment are not provided by this helper. Pipelines requiring these semantics need a
different topology and should not assume the helper preserves them.

### StreamingPull Connections and Flow Control

Each Pub/Sub source subtask opens and manages
[`StreamingPull`](https://cloud.google.com/pubsub/docs/pull#streamingpull_api)
connections to Google Cloud Pub/Sub. The number of connections per subtask can
be set using `PubSubSource.<OutputT>builder().setParallelPullCount` (defaults to
1). Opening more connections can increase the message throughput delivered to
each subtask. Note that the underlying subscriber client library creates an
executor with 5 threads for each connection opened, so too many connections can
be detrimental to performance.

Google Cloud Pub/Sub servers pause message delivery to a `StreamingPull`
connection when a flow control limit is exceeded. There are two forms of flow
control:

1) Message delivery throughput
2) Outstanding message count / bytes

`StreamingPull` connections are limited to pulling messages at 10 MB/s. This
limit cannot be configured. Opening more connections is recommended when
observing message throughput flow control. See
[Pub/Sub quotas and limits](https://cloud.google.com/pubsub/quotas) for a full
list of limitations.

The other form of flow control is based on outstanding messages--when a message
has been delivered but not yet acknowledged. Since outstanding messages are
acknowledged when a checkpoint completes, flow control limits for outstanding
messages are effectively per-checkpoint interval limits. Infrequent
checkpointing can cause connections to be flow controlled due to too many
outstanding messages.

The snippet below shows how to configure connection count and flow control
settings.

```java
PubSubSource.<OutputT>builder()
    // Open 5 StreamingPull connections.
    .setParallelPullCount(5)
    // Allow up to 10,000 message deliveries per checkpoint interval.
    .setMaxOutstandingMessagesCount(10_000L)
    // Allow up to 1000 MB in cumulatitive message size per checkpoint interval.
    .setMaxOutstandingMessagesBytes(1000L * 1024L * 1024L)  // 1000 MB
```

A Pub/Sub source subtask with these options is able to:

- Pull messages at up to 50 MB/s
- Receive up to 50,000 messages **or** 5000 MB in cumulative message size per
  checkpoint interval

### Message Leasing

Pub/Sub source automatically
[extends the acknowledgement deadline](https://cloud.google.com/pubsub/docs/lease-management)
of messages. This means a checkpointing interval can be longer than a message's
acknowledgement deadline without causing message redelivery. Note that message
acknowledgement deadlines are extended for up to 1 hour, after which, they are
redelivered by Google Cloud Pub/Sub.

### All Options

#### Required Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setSubscriptionName(String subscriptionName)</td>
        <td>(none)</td>
        <td>The ID of the subscription from which Pub/Sub source consumes messages.</td>
    </tr>
    <tr>
        <td>setProjectName(String projectName)</td>
        <td>(none)</td>
        <td>The ID of the GCP project that owns the subscription from which Pub/Sub source consumes messages.</td>
    </tr>
    <tr>
        <td>setDeserializationSchema(PubSubDeserializationSchema&lt;OutputT&gt; deserializationSchema)</td>
        <td>(none)</td>
        <td>How <code>PubsubMessage</code> is deserialized when Pub/Sub source receives a message.</td>
    </tr>
  </tbody>
</table>

#### Optional Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setMaxOutstandingMessagesCount(Long count)</td>
        <td><code>1000L</code></td>
        <td>The maximum number of messages that can be delivered to a StreamingPull connection within a checkpoint interval.</td>
    </tr>
    <tr>
        <td>setMaxOutstandingMessagesBytes(Long bytes)</td>
        <td><code>100L * 1024L * 1024L</code> (100 MB)</td>
        <td>The maximum number of cumulative bytes that can be delivered to a StreamingPull connection within a checkpoint interval.</td>
    </tr>
    <tr>
        <td>setParallelPullCount(Integer parallelPullCount)</td>
        <td>1</td>
        <td>The number of StreamingPull connections to open for pulling messages from Google Cloud Pub/Sub.</td>
    </tr>
    <tr>
        <td>setCredentials(Credentials credentials)</td>
        <td>(none)</td>
        <td>The credentials attached to requests sent to Google Cloud Pub/Sub. The identity in the credentials must be authorized to pull messages from the subscription. If not set, then Pub/Sub source uses Application Default Credentials.</td>
    </tr>
    <tr>
        <td>setEndpoint(String endpoint)</td>
        <td>pubsub.googleapis.com:443</td>
        <td>The Google Cloud Pub/Sub gRPC endpoint from which messages are pulled. Defaults to the global endpoint, which routes requests to the nearest regional endpoint.</td>
    </tr>
    <tr>
        <td>setReplaySnapshotRetentionCount(Integer count)</td>
        <td>(disabled)</td>
        <td>Enables checkpoint-coordinated replay snapshots. Retains the most recent count completed provider boundaries plus one shared pending boundary, bounded by count + 1 provider snapshots. Requires count &gt; 0, snapshot permissions, and a dedicated subscription. Snapshot storage is billed separately; replay remains at least once.</td>
    </tr>
  </tbody>
</table>

## Pub/Sub Sink

Each sink writer owns a publisher and two bounded pools: two publishing/retry threads
and two transport callback threads. Pools are not shared across writers.

Closing a sink writer waits at most 30 seconds for flushing pending publishes and
shutting down the publisher. A timeout or task-thread interruption is reported as
an error; pending publishes are not reported as successful. The writer owns its
transport, retry scheduler, and callback executor. On timeout or interruption it
aborts the transport and retry scheduler; callbacks remain runnable until pending
publishes finish failing, then the cleanup worker closes their executor. Normal
publish retries are unchanged. Normal checkpoint flushes still wait for publish
results and report publish failures. Cleanup workers have unique names; if the
caller stops waiting, logs report the abort and eventual cleanup result.

Pub/Sub sink publishes data to a single Google Cloud Pub/Sub topic with an
at-least-once guarantee. The sample below shows the minimal configurations
required to build Pub/Sub sink.

```java
PubSubSink.<String>builder()
                .setSerializationSchema(
                    PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName("my-project-name")
                .setTopicName("my-topic-name")
                .build()
```

### Serialization Schema

`PubSubSerializationSchema<T>` is required to define how incoming data is
serialized to
[`PubsubMessage`](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage).

For convenience,
`PubSubSerializationSchema.dataOnly(SerializationSchema<T> schema)` can be used
to write output data to the field `PubsubMessage.data`. The type of
`SerializationSchema<T>` must be one of the supported types matching
[`ByteString.copyFrom(schema.serialize(T ...))`](https://cloud.google.com/java/docs/reference/protobuf/latest/com.google.protobuf.ByteString).


Implementing `PubSubSerializationSchema<T>` is required to publish messages with
attributes or ordering keys.

<!-- TODO(matt-kwong): Add custom serialization code example. -->

### All Options

#### Required Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setTopicName(String topicName)</td>
        <td>(none)</td>
        <td>The ID of the topic to which Pub/Sub sink publishes messages.</td>
    </tr>
    <tr>
        <td>setProjectName(String projectName)</td>
        <td>(none)</td>
        <td>The ID of the GCP project that owns the topic to which Pub/Sub sink publishes messages.</td>
    </tr>
    <tr>
        <td>setSerializationSchema(PubSubSerializationSchema&lt;T&gt; serializationSchema)</td>
        <td>(none)</td>
        <td>How incoming data is serialized to <code>PubsubMessage</code>.</td>
    </tr>
  </tbody>
</table>

#### Optional Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setCredentials(Credentials credentials)</td>
        <td>(none)</td>
        <td>The credentials attached to requests sent to Google Cloud Pub/Sub. The identity in the credentials must be authorized to publish messages to the topic. If not set, then Pub/Sub sink uses Application Default Credentials.</td>
    </tr>
    <tr>
        <td>setEnableMessageOrdering(Boolean enableMessageOrdering)</td>
        <td>false</td>
        <td>This must be set to true when publishing messages with an ordering key.</td>
    </tr>
    <tr>
        <td>setEndpoint(String endpoint)</td>
        <td>pubsub.googleapis.com:443</td>
        <td>The Google Cloud Pub/Sub gRPC endpoint to which messages are published. Defaults to the global endpoint, which routes requests to the nearest regional endpoint.</td>
    </tr>
  </tbody>
</table>

## Source diagnostics

Each source reader registers the following metrics under its `pubsub` metric group:

| Metric | Meaning |
| --- | --- |
| `outstandingMessages` | Distinct messages owned by the reader until its ACK callbacks return. |
| `outstandingDeliveryHandles` | Unresolved delivery handles, including suppressed redeliveries. |
| `pendingMessages` | Owned messages that have not been emitted downstream. |
| `stagedMessages` | Emitted messages awaiting inclusion in a checkpoint. |
| `checkpointAckBatches` | Checkpoint batches awaiting ACK callbacks. |
| `oldestOutstandingAgeMillis` | Age since first local delivery of the oldest owned message. |
| `redeliveries` | Repeated deliveries while the reader still owns that message. |
| `checkpointAckFailures` | Checkpoint-completion attempts interrupted by an ACK callback exception. |

Suppressed redeliveries retain their delivery handles until checkpoint completion
or shutdown. They are not acknowledged early, and no immediate NACK is sent:
a stale NACK can trigger another delivery on a standard subscription. These extra
handles consume SDK flow-control capacity until resolved; compare
`outstandingDeliveryHandles` with the distinct `outstandingMessages` count.

The `pubsub.ack` and `pubsub.nack` groups separately report `requests`, `inFlight`,
`oldestInFlightAgeMillis`, `callbackFailures`, and `responseFailures`.
Their `responses` subgroups count SDK outcomes: `successful`, `invalid`,
`permission_denied`, `failed_precondition`, and `other`. `responseFailures` also
includes futures that fail exceptionally or are cancelled. These observations are
asynchronous and do not block checkpoint completion or change replay ownership.

**SDK success is not always broker confirmation.** When exactly-once delivery is
disabled on the subscription, the SDK returns immediate success. Server-confirmed
outcomes require exactly-once delivery on the subscription; this connector does not
enable it automatically. See the
[SDK acknowledgement contract](https://cloud.google.com/java/docs/reference/google-cloud-pubsub/latest/com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse).

These gauges describe local reader ownership and SDK operations, not the complete
Pub/Sub subscription backlog or messages still buffered inside the SDK. Metrics do
not contain message IDs, acknowledgement IDs, or payloads.

### Replay coordinator diagnostics

The coordinator's `pubsub` group exposes `snapshotCreateFailures`,
`snapshotDeleteFailures`, `pendingSnapshotDeletions`, and
`replaySnapshotTimeToExpiryMillis`. The expiry gauge is cached metadata for the
selected replay boundary; reporters do not issue provider RPCs. Watch it when
creation is repeatedly falling back to an older snapshot, including after rollback
with a full resource budget. Alert before the remaining lifetime becomes shorter than
your expected recovery and operator response time. Expiry remains a hard
limit on recovery, even when checkpoints continue to ACK input.

## Integration Testing

Instead of integration tests reading from and writing to production Google Cloud
Pub/Sub, tests can run against a local instance of the
[Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator). Pub/Sub
source and sink will automatically try connecting to the emulator if the
environment variable `PUBSUB_EMULATOR_HOST` is set. Alternatively, you can
manually set the emulator endpoint in your builder by calling
`.setEndpoint(EmulatorEndpoint.toEmulatorEndpoint("localhost:8085"))`.

Steps to run tests against the Pub/Sub emulator:

1.  Ensure that the
    [required dependencies](https://cloud.google.com/pubsub/docs/emulator#before-you-begin)
    and
    [emulator](https://cloud.google.com/pubsub/docs/emulator#install_the_emulator)
    are installed.
2.  Start the emulator using the
    [Google Cloud CLI](https://cloud.google.com/pubsub/docs/emulator#start).
3.  Run the test with the environment variable `PUBSUB_EMULATOR_HOST` set to
    where the emulator is running. For example, if the emulator is listening on
    port 8085 and running on the same machine as the test, set
    `PUBSUB_EMULATOR_HOST=localhost:8085`.

The emulator can also be started within a Docker container while testing. The
tests under `flink-connector/flink-connector-gcp-pubsub-e2e-tests/` illustrate
how to do this.

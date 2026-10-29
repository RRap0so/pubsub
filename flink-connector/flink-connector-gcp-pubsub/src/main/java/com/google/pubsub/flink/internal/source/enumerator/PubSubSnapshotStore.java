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
package com.google.pubsub.flink.internal.source.enumerator;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.pubsub.flink.util.EmulatorEndpoint;
import com.google.pubsub.v1.ProjectSnapshotName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.SeekRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Administrative Pub/Sub operations used only when replay recovery is explicitly enabled. */
public final class PubSubSnapshotStore implements PubSubReplaySnapshots.Store {
  private final String project;
  private final ProjectSubscriptionName subscription;
  private final SubscriptionAdminClient client;
  private final ManagedChannel channel;

  public PubSubSnapshotStore(
      String project,
      String subscription,
      Optional<Credentials> credentials,
      Optional<String> endpoint)
      throws IOException {
    this.project = project;
    this.subscription = ProjectSubscriptionName.of(project, subscription);
    SubscriptionAdminSettings.Builder settings = SubscriptionAdminSettings.newBuilder();
    if (credentials.isPresent()) {
      settings.setCredentialsProvider(FixedCredentialsProvider.create(credentials.get()));
    }
    String emulator = EmulatorEndpoint.getEmulatorEndpoint(endpoint);
    if (emulator != null) {
      channel = ManagedChannelBuilder.forTarget(emulator).usePlaintext().build();
      settings.setCredentialsProvider(NoCredentialsProvider.create());
      settings.setTransportChannelProvider(
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
    } else {
      channel = null;
      if (endpoint.isPresent()) {
        settings.setEndpoint(endpoint.get());
      }
    }
    try {
      client = SubscriptionAdminClient.create(settings.build());
    } catch (IOException | RuntimeException failure) {
      if (channel != null) {
        channel.shutdownNow();
      }
      throw failure;
    }
  }

  @Override
  public String create(String name) {
    return client.createSnapshot(snapshotName(name), subscription).getName();
  }

  @Override
  public void seek(String name) {
    client.seek(
        SeekRequest.newBuilder()
            .setSubscription(subscription.toString())
            .setSnapshot(snapshotName(name).toString())
            .build());
  }

  @Override
  public void delete(String name) {
    try {
      client.deleteSnapshot(snapshotName(name));
    } catch (NotFoundException alreadyGone) {
      // Expired resources have already released their retained backlog.
    }
  }

  @Override
  public Iterable<String> list() {
    List<String> names = new ArrayList<>();
    client
        .listSnapshots("projects/" + project)
        .iterateAll()
        .forEach(snapshot -> names.add(snapshot.getName()));
    return names;
  }

  private ProjectSnapshotName snapshotName(String name) {
    ProjectSnapshotName parsed =
        name.contains("/")
            ? ProjectSnapshotName.parse(name)
            : ProjectSnapshotName.of(project, name);
    Preconditions.checkArgument(
        project.equals(parsed.getProject()), "Replay snapshot belongs to a different project");
    return parsed;
  }

  @Override
  public void close() {
    try {
      client.close();
    } finally {
      if (channel != null) {
        channel.shutdownNow();
      }
    }
  }
}

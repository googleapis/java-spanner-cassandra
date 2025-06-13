/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.google.cloud.spanner.adapter;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.google.cloud.spanner.adapter.opentelemetry.BuiltInRequestTracker;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to build a {@link CqlSession} instance that connects to Google Cloud Spanner.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public final class SpannerCqlSessionBuilder
    extends SessionBuilder<SpannerCqlSessionBuilder, SpannerCqlSession> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerCqlSessionBuilder.class);
  private static final int DEFAULT_PORT = 9042;
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int DEFAULT_NUM_GRPC_CHANNELS = 4;

  private InetAddress iNetAddress;
  private int port;
  private Adapter adapter;
  private int numGrpcChannels = DEFAULT_NUM_GRPC_CHANNELS;
  private String databaseUri = null;

  /**
   * Wraps the default CQL session with a SpannerCqlSession instance.
   *
   * @param defaultSession The default CQL session.
   * @return A SpannerCqlSession instance.
   */
  @Override
  protected SpannerCqlSession wrap(CqlSession defaultSession) {
    return new SpannerCqlSession(defaultSession, adapter);
  }

  /**
   * Sets the URI of the Google Cloud Spanner database to connect to.
   *
   * @param databaseUri The database URI.
   * @return This builder instance.
   */
  public SpannerCqlSessionBuilder setDatabaseUri(String databaseUri) {
    this.databaseUri = databaseUri;
    return this;
  }

  public SpannerCqlSessionBuilder setNumGrpcChannels(int numGrpcChannels) {
    this.numGrpcChannels = numGrpcChannels;
    return this;
  }

  /**
   * Sets whether to enable or disable built in metrics. Built in metrics are disabled by default.
   */
  public SpannerCqlSessionBuilder setBuiltInMetricsEnabled(boolean enableBuiltInMetrics) {
    if (enableBuiltInMetrics) {
      this.addRequestTracker(BuiltInRequestTracker.INSTANCE);
    }
    return this;
  }

  /**
   * Creates the session with the options set by this builder.
   *
   * <p>The session initialization will happen asynchronously in a driver internal thread pool.
   * Starts the Adapter before building the CQL session.
   *
   * @return A completion stage that completes with the session when it is fully initialized.
   */
  @Override
  public CompletionStage<SpannerCqlSession> buildAsync() {

    try {
      checkAdapterSettings();
    } catch (UnknownHostException | RuntimeException e) {
      CompletableFuture<SpannerCqlSession> exceptionallyCompletedFuture = new CompletableFuture<>();
      exceptionallyCompletedFuture.completeExceptionally(e);
      return exceptionallyCompletedFuture;
    }

    // Start the Adapter asynchronously.
    CompletableFuture<Void> adapterStartFuture =
        CompletableFuture.runAsync(this::createAndStartAdapter);

    // After the Adapter starts, build the CQL session.
    return adapterStartFuture.thenCompose(
        v -> {
          LOG.info("Creating CQL session for database URI: {}", databaseUri);
          return super.buildAsync();
        });
  }

  private void checkAdapterSettings() throws UnknownHostException {
    checkDatabaseUri();
    checkContactPoints();
    checkNumGrpcChannels();
  }

  private void checkDatabaseUri() {
    if (databaseUri == null) {
      throw new NullPointerException("Spanner database URI must be set.");
    }
  }

  private void checkContactPoints() throws UnknownHostException {
    if (programmaticContactPoints.size() > 1) {
      throw new IllegalArgumentException(
          "At most one contact point can be provided when using SpannerCqlSessionBuilder, as it"
              + " connects to the local adapter.");
    }

    if (programmaticContactPoints.isEmpty()) {
      iNetAddress = InetAddress.getByName(DEFAULT_HOST);
      port = DEFAULT_PORT;
      LOG.info(
          "No contact point provided. Defaulting to connect adapter via {}:{}",
          DEFAULT_HOST,
          DEFAULT_PORT);
      return;
    }

    // Exactly one contact point was provided by the user. Validate it.
    EndPoint providedEndPoint = programmaticContactPoints.iterator().next();
    SocketAddress address = providedEndPoint.resolve();

    if (address instanceof InetSocketAddress) {
      InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
      iNetAddress = inetSocketAddress.getAddress();
      port = inetSocketAddress.getPort();
      LOG.debug("Using user-provided localhost contact point: {}", inetSocketAddress);
    } else {
      // Should not happen with typical Cassandra contact points.
      throw new IllegalArgumentException(
          "Unsupported contact point address type: " + address.getClass().getName());
    }
  }

  private void checkNumGrpcChannels() {
    if (numGrpcChannels <= 0) {
      throw new IllegalArgumentException("Number of gRPC channels should be greater than 0.");
    }
  }

  private void createAndStartAdapter() {
    adapter = new Adapter(databaseUri, iNetAddress, port, numGrpcChannels);
    adapter.start();
  }
}

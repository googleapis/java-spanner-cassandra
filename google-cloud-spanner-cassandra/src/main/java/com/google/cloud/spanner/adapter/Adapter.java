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

import static com.google.cloud.spanner.adapter.util.ThreadFactoryUtil.tryCreateVirtualThreadPerTaskExecutor;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GaxProperties;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages client connections, acting as an intermediary for communication with Spanner. */
@NotThreadSafe
final class Adapter {
  private static final Logger LOG = LoggerFactory.getLogger(Adapter.class);
  private static final String RESOURCE_PREFIX_HEADER_KEY = "google-cloud-resource-prefix";
  private static final long MAX_GLOBAL_STATE_SIZE = (long) (1e8 / 256); // ~100 MB
  private static final int DEFAULT_CONNECTION_BACKLOG = 50;
  private static final String ENV_VAR_GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS =
      "GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS";
  private static final String USER_AGENT_KEY = "user-agent";
  private static final String CLIENT_LIBRARY_LANGUAGE = "java-spanner-cassandra";
  private static final String CLIENT_VERSION = "0.3.0"; // {x-release-please-version}
  public static final String DEFAULT_USER_AGENT =
      CLIENT_LIBRARY_LANGUAGE
          + "/v"
          + CLIENT_VERSION
          + GaxProperties.getLibraryVersion(Adapter.class);
  private static final ImmutableSet<String> SCOPES =
      ImmutableSet.of(
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/spanner.data");

  private AdapterClientWrapper adapterClientWrapper;
  private AdapterClient adapterClient;
  private ServerSocket serverSocket;
  private ExecutorService executor;
  private boolean started = false;
  private AdapterOptions options;

  /**
   * Constructor for the Adapter class, specifying a specific address to bind to.
   *
   * @param options options for init.
   */
  Adapter(AdapterOptions options) {
    this.options = options;
  }

  /** Starts the adapter, initializing the local TCP server and handling client connections. */
  void start() {
    if (started) {
      return;
    }

    try {
      Credentials credentials = options.getCredentials();
      if (credentials == null) {
        credentials = GoogleCredentials.getApplicationDefault();
      }
      final CredentialsProvider credentialsProvider = setUpCredentialsProvider(credentials);

      InstantiatingGrpcChannelProvider.Builder channelProviderBuilder =
          AdapterSettings.defaultGrpcTransportProviderBuilder();

      if (options.getUseVirtualThreads()) {
        executor = tryCreateVirtualThreadPerTaskExecutor("spanner-virtual-thread");
        channelProviderBuilder.setExecutor(executor);
      }

      channelProviderBuilder
          .setAllowNonDefaultServiceAccount(true)
          .setChannelPoolSettings(
              ChannelPoolSettings.staticallySized(options.getNumGrpcChannels()));

      if (isEnableDirectPathXdsEnv()) {
        channelProviderBuilder.setAttemptDirectPath(true);
        // This will let the credentials try to fetch a hard-bound access token if the runtime
        // environment supports it.
        channelProviderBuilder.setAllowHardBoundTokenTypes(
            Collections.singletonList(InstantiatingGrpcChannelProvider.HardBoundTokenTypes.ALTS));
        channelProviderBuilder.setAttemptDirectPathXds();
      }
      final HeaderProvider headerProvider =
          FixedHeaderProvider.create(
              RESOURCE_PREFIX_HEADER_KEY,
              options.getDatabaseUri(),
              USER_AGENT_KEY,
              DEFAULT_USER_AGENT);
      AdapterSettings settings =
          AdapterSettings.newBuilder()
              .setEndpoint(options.getSpannerEndpoint())
              .setTransportChannelProvider(
                  MoreObjects.firstNonNull(
                      options.getChannelProvider(), channelProviderBuilder.build()))
              .setCredentialsProvider(credentialsProvider)
              .setHeaderProvider(headerProvider)
              .build();

      adapterClient = AdapterClient.create(settings);

      AttachmentsCache attachmentsCache = new AttachmentsCache(MAX_GLOBAL_STATE_SIZE);
      SessionManager sessionManager = new SessionManager(adapterClient, options.getDatabaseUri());

      // Create initial session to verify database existence
      sessionManager.getSession();

      adapterClientWrapper =
          new AdapterClientWrapper(adapterClient, attachmentsCache, sessionManager);

      // Start listening on the specified host and port.
      serverSocket =
          new ServerSocket(
              options.getTcpPort(), DEFAULT_CONNECTION_BACKLOG, options.getInetAddress());
      LOG.info("Local TCP server started on {}:{}", options.getInetAddress(), options.getTcpPort());

      if (executor == null) {
        executor = Executors.newCachedThreadPool();
      }

      // Start accepting client connections.
      executor.execute(this::acceptClientConnections);

      started = true;
      LOG.info("Adapter started for database '{}'.", options.getDatabaseUri());

    } catch (IOException | RuntimeException e) {
      throw new AdapterStartException(e);
    }
  }

  /**
   * Stops the adapter, shutting down the executor, closing the server socket, and closing the
   * adapter client.
   *
   * @throws IOException If an I/O error occurs while closing the server socket.
   */
  void stop() throws IOException {
    if (!started) {
      throw new IllegalStateException("Adapter was never started!");
    }
    executor.shutdownNow();
    adapterClient.close();
    serverSocket.close();
    LOG.info("Adapter stopped.");
  }

  private void acceptClientConnections() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        final Socket socket = serverSocket.accept();
        // Optimize for latency (2), then bandwidth (1) and then connection time (0).
        socket.setPerformancePreferences(0, 2, 1);
        // Turn on TCP_NODELAY to optimize for chatty protocol that prefers low latency.
        socket.setTcpNoDelay(true);
        executor.execute(
            new DriverConnectionHandler(socket, adapterClientWrapper, options.getMaxCommitDelay()));
        LOG.debug("Accepted client connection from: {}", socket.getRemoteSocketAddress());
      }
    } catch (SocketException e) {
      if (!serverSocket.isClosed()) {
        LOG.error("Error accepting client connection", e);
      }
    } catch (IOException e) {
      LOG.error("Error accepting client connection", e);
    }
  }

  private static CredentialsProvider setUpCredentialsProvider(final Credentials credentials) {
    Credentials scopedCredentials = getScopedCredentials(credentials);
    if (scopedCredentials != null && scopedCredentials != NoCredentials.getInstance()) {
      return FixedCredentialsProvider.create(scopedCredentials);
    }
    return NoCredentialsProvider.create();
  }

  private static Credentials getScopedCredentials(final Credentials credentials) {
    Credentials credentialsToReturn = credentials;
    if (credentials instanceof GoogleCredentials
        && ((GoogleCredentials) credentials).createScopedRequired()) {
      credentialsToReturn = ((GoogleCredentials) credentials).createScoped(SCOPES);
    }
    return credentialsToReturn;
  }

  private static boolean isEnableDirectPathXdsEnv() {
    return Boolean.parseBoolean(System.getenv(ENV_VAR_GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS));
  }

  private static final class AdapterStartException extends RuntimeException {
    public AdapterStartException(Throwable cause) {
      super("Failed to start the adapter.", cause);
    }
  }
}

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

import com.google.api.core.AbstractApiService;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Collections;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages client connections, acting as an intermediary for communication with Spanner. */
@NotThreadSafe
final class Adapter extends AbstractApiService {
  private static final Logger LOG = LoggerFactory.getLogger(Adapter.class);
  private static final String RESOURCE_PREFIX_HEADER_KEY = "google-cloud-resource-prefix";
  private static final long MAX_GLOBAL_STATE_SIZE = (long) (1e8 / 256); // ~100 MB
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
  private AdapterOptions options;
  private AsynchronousServerSocketChannel listener;

  /**
   * Constructor for the Adapter class, specifying a specific address to bind to.
   *
   * @param options options for init.
   */
  Adapter(AdapterOptions options) {
    this.options = options;
  }

  private void acceptClientConnections(InetAddress inetAddress, int port) throws IOException {
    // 1. Create an asynchronous server socket channel
    LOG.info("Starting listening...");
    listener =
        AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(inetAddress, port));
    LOG.info("Started listening on {}:{}", inetAddress, port);

    // 2. Create a CompletionHandler to handle new connections
    CompletionHandler<AsynchronousSocketChannel, Void> acceptHandler =
        new CompletionHandler<AsynchronousSocketChannel, Void>() {
          @Override
          public void completed(AsynchronousSocketChannel channel, Void attachment) {
            // A client connected!
            // Immediately start listening for the *next* connection
            listener.accept(null, this);

            // Handle the newly connected client
            new DriverConnectionHandler(channel, adapterClientWrapper, options.getMaxCommitDelay())
                .startReading();
          }

          @Override
          public void failed(Throwable exc, Void attachment) {
            LOG.error("Error accepting client connection", exc);
          }
        };

    // 3. Start listening for the first connection
    listener.accept(null, acceptHandler);
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

  @Override
  protected void doStart() {
    try {
      Credentials credentials = options.getCredentials();
      if (credentials == null) {
        credentials = GoogleCredentials.getApplicationDefault();
      }
      final CredentialsProvider credentialsProvider = setUpCredentialsProvider(credentials);

      InstantiatingGrpcChannelProvider.Builder channelProviderBuilder =
          AdapterSettings.defaultGrpcTransportProviderBuilder();

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

      AdapterClient adapterClient = AdapterClient.create(settings);

      AttachmentsCache attachmentsCache = new AttachmentsCache(MAX_GLOBAL_STATE_SIZE);
      SessionManager sessionManager = new SessionManager(adapterClient, options.getDatabaseUri());

      // Create initial session to verify database existence
      sessionManager.getSession();

      adapterClientWrapper =
          new AdapterClientWrapper(adapterClient, attachmentsCache, sessionManager);

      acceptClientConnections(options.getInetAddress(), options.getTcpPort());

      LOG.info("Adapter started for database '{}'.", options.getDatabaseUri());

    } catch (IOException | RuntimeException e) {
      throw new AdapterStartException(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      listener.close();
    } catch (IOException e) {
      LOG.error("Error stopping adapter", e);
    }
  }
}

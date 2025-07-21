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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.io.IOException;
import java.util.Collections;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Manages client connections, acting as an intermediary for communication with Spanner. */
@NotThreadSafe
final class Adapter {
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

  // Netty Event Loop Groups for non-blocking I/O
  private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private Channel serverChannel;

  private AttachmentsCache attachmentsCache;
  private AdapterClient adapterClient;
  private SessionManager sessionManager;
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

  /** Starts the adapter, initializing the non-blocking TCP server. */
  void start() {
    if (started) {
      return;
    }

    try {
      // The gRPC client setup remains the same.
      Credentials credentials = options.getCredentials();
      if (credentials == null) {
        credentials = GoogleCredentials.getApplicationDefault();
      }
      final CredentialsProvider credentialsProvider = setUpCredentialsProvider(credentials);

      InstantiatingGrpcChannelProvider.Builder channelProviderBuilder =
          AdapterSettings.defaultGrpcTransportProviderBuilder();

      channelProviderBuilder
          .setAllowNonDefaultServiceAccount(true)
          .setKeepAliveTime(Duration.ofMinutes(2))
          .setKeepAliveWithoutCalls(true)
          .setChannelPoolSettings(
              ChannelPoolSettings.staticallySized(options.getNumGrpcChannels()));

      if (isEnableDirectPathXdsEnv()) {
        channelProviderBuilder.setAttemptDirectPath(true);
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
      attachmentsCache = new AttachmentsCache(MAX_GLOBAL_STATE_SIZE);
      sessionManager = new SessionManager(adapterClient, options.getDatabaseUri());
      sessionManager.getSession(); // Create initial session to verify database.

      // Configure the server using Netty's bootstrap.
      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap
          .group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(
              new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel ch) {
                  ch.pipeline()
                      .addLast(
                          // This decoder handles message framing automatically based on the length
                          // field in the header. This replaces constructPayload().
                          // Max frame size: 128MB, length field offset: 5, length field size: 4.
                          new LengthFieldBasedFrameDecoder(128 * 1024 * 1024, 5, 4, 0, 0),
                          // The new handler replaces the old Runnable.
                          new DriverConnectionHandler(
                              adapterClient,
                              sessionManager,
                              attachmentsCache,
                              options.getMaxCommitDelay()));
                }
              })
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_KEEPALIVE, true);

      // Bind and start to accept incoming connections.
      serverChannel =
          bootstrap.bind(options.getInetAddress(), options.getTcpPort()).sync().channel();
      LOG.info("Adapter started. Listening for connections on {}", serverChannel.localAddress());
      started = true;

    } catch (IOException | RuntimeException | InterruptedException e) {
      throw new AdapterStartException(e);
    }
  }

  /** Stops the adapter, shutting down the Netty event loops and closing the adapter client. */
  void stop() {
    if (!started) {
      throw new IllegalStateException("Adapter was never started!");
    }
    try {
      serverChannel.close().awaitUninterruptibly();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
    LOG.info("Adapter stopped.");
  }

  // Helper methods remain the same...
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

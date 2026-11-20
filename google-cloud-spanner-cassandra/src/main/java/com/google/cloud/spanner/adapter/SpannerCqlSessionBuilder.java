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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsProvider;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsRecorder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.google.spanner.adapter.v1.DatabaseName;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to build a {@link CqlSession} instance that connects to Google Cloud Spanner.
 *
 * <p>When no contact point is configured, the embedded in-process adapter binds to a loopback
 * address ({@code 127.0.0.1} or {@code ::1}) on an OS-assigned ephemeral port ({@code 0}). Any
 * user-supplied contact point must resolve to a loopback address; non-loopback or unresolved
 * contact points are rejected with {@link IllegalArgumentException}.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public final class SpannerCqlSessionBuilder
    extends SessionBuilder<SpannerCqlSessionBuilder, SpannerCqlSession> {

  /** Specifies the type of Spanner instance to connect to (CLOUD or OMNI). */
  public enum InstanceType {
    CLOUD,
    OMNI
  }

  private static final Logger LOG = LoggerFactory.getLogger(SpannerCqlSessionBuilder.class);
  private final BuiltInMetricsProvider builtInMetricsProvider = BuiltInMetricsProvider.INSTANCE;
  private static final int DEFAULT_PORT = 0;
  private static final int DEFAULT_NUM_GRPC_CHANNELS = 4;
  private static final int LARGEST_MAX_COMMIT_DELAY_MILLIS = 500;
  private static final String DEFAULT_SPANNER_ENDPOINT = "spanner.googleapis.com:443";
  private static final String ENV_VAR_SPANNER_ENDPOINT = "SPANNER_ENDPOINT";
  private static final String SPANNER_OMNI_ID = "default";

  private InetAddress iNetAddress;
  private int port;
  // registeredAdapterEndPoint is builder-owned: it replaces userContactPoint in
  // programmaticContactPoints only between createAndStartAdapter() and the next
  // checkContactPoints().
  private EndPoint userContactPoint;
  private EndPoint registeredAdapterEndPoint;
  private boolean configContactPointProvided;
  private Adapter adapter;
  private int numGrpcChannels = DEFAULT_NUM_GRPC_CHANNELS;
  private String databaseUri = null;
  private String host = null;
  private Optional<Duration> maxCommitDelay = Optional.empty();
  private boolean enableBuiltInMetrics = false;
  private TransportChannelProvider channelProvider = null;
  private Credentials credentials;
  private boolean useVirtualThreads;
  private boolean usePlainText;
  private InstanceType instanceType = InstanceType.CLOUD;
  private String clientCertPath = null;
  private String clientKeyPath = null;

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
   * Sets the spanner host.
   *
   * @param host The spanner host.
   * @return This builder instance.
   */
  public SpannerCqlSessionBuilder setHost(String host) {
    this.host = host;
    return this;
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

  /** Sets the number of gRPC channels to use. By default 4 channels are created. */
  public SpannerCqlSessionBuilder setNumGrpcChannels(int numGrpcChannels) {
    this.numGrpcChannels = numGrpcChannels;
    return this;
  }

  // TODO: Add a code sample for setting this option.
  /**
   * Sets the max commit delay to use in requests. This will apply globally to all Batch and Execute
   * DML requests. By default this argument is not set.
   */
  public SpannerCqlSessionBuilder setMaxCommitDelay(Duration maxCommitDelay) {
    this.maxCommitDelay = Optional.of(maxCommitDelay);
    return this;
  }

  /**
   * Sets whether to enable or disable built in metrics. Built in metrics are disabled by default.
   */
  public SpannerCqlSessionBuilder setBuiltInMetricsEnabled(boolean enableBuiltInMetrics) {
    this.enableBuiltInMetrics = enableBuiltInMetrics;
    return this;
  }

  /** Sets the gRPC transport channel provider. */
  public SpannerCqlSessionBuilder setChannelProvider(TransportChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
    return this;
  }

  /** Sets the GCP credentials for accessing Cloud Spanner. */
  public SpannerCqlSessionBuilder setGoogleCloudCredentials(Credentials credentials) {
    this.credentials = credentials;
    return this;
  }

  /**
   * (Optional, default `false`) Enables/disables the use of virtual threads for the gRPC executor.
   * Setting this option only has any effect on Java 21 and higher. In all other cases, the option
   * will be ignored.
   */
  protected SpannerCqlSessionBuilder setUseVirtualThreads(boolean useVirtualThreads) {
    this.useVirtualThreads = useVirtualThreads;
    return this;
  }

  /**
   * (Optional, default `false`) Enables/disables plaintext connections to the Spanner endpoint.
   * This should only be used for testing against a local fake Spanner instance.
   */
  public SpannerCqlSessionBuilder setUsePlainText(boolean usePlainText) {
    this.usePlainText = usePlainText;
    return this;
  }

  /**
   * (Optional, default CLOUD) Specifies the type of Spanner instance to connect to. Setting it to
   * OMNI is mandatory when connecting to a Spanner Omni instance.
   */
  public SpannerCqlSessionBuilder setInstanceType(InstanceType instanceType) {
    this.instanceType = instanceType;
    return this;
  }

  /**
   * (Optional, default null) Experimental Host endpoint.
   *
   * @deprecated Use {@link #setHost(String)} and {@link #setInstanceType(InstanceType)} instead.
   */
  @Deprecated
  public SpannerCqlSessionBuilder setExperimentalHostEndpoint(String experimentalHostEndpoint) {
    if (!Strings.isNullOrEmpty(experimentalHostEndpoint)) {
      this.host = experimentalHostEndpoint;
      this.instanceType = InstanceType.OMNI;
    }
    return this;
  }

  /**
   * (Optional, default null) Enables mTLS connection to Spanner Omni instance using client
   * certificate and key. This should only be used for connecting to Spanner Omni instances.
   */
  public SpannerCqlSessionBuilder setClientCertPathAndKey(
      String clientCertPath, String clientKeyPath) {
    this.clientCertPath = clientCertPath;
    this.clientKeyPath = clientKeyPath;
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

    // After the Adapter starts, build the CQL session. If session initialization fails, stop the
    // started Adapter so its ServerSocket, gRPC channels, and executor threads do not leak.
    return adapterStartFuture.thenCompose(
        v -> {
          LOG.info("Creating CQL session for database URI: {}", databaseUri);
          final Adapter startedAdapter = this.adapter;
          final CompletionStage<SpannerCqlSession> sessionFuture;
          try {
            sessionFuture = super.buildAsync();
          } catch (Throwable t) {
            stopAdapterQuietly(startedAdapter);
            throw t;
          }
          return sessionFuture.whenComplete(
              (session, error) -> {
                if (error != null) {
                  stopAdapterQuietly(startedAdapter);
                }
              });
        });
  }

  /**
   * Builds the {@link DriverContext} for this session.
   *
   * <p>When no contact point was explicitly supplied by the caller, {@link #createAndStartAdapter}
   * registers the OS-assigned ephemeral loopback endpoint in {@code programmaticContactPoints},
   * which would otherwise cause {@link MetadataManager#wasImplicitContactPoint()} to return {@code
   * false}. Returning {@code true} here preserves pre-ephemeral-port driver behavior: explicit
   * programmatic or config-supplied local datacenters take precedence, while zero-configuration
   * callers (including named profiles) still infer the local datacenter from the server.
   */
  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    if (userContactPoint == null && !configContactPointProvided) {
      return new DefaultDriverContext(configLoader, programmaticArguments) {
        @Override
        protected MetadataManager buildMetadataManager() {
          return new MetadataManager(this) {
            @Override
            public boolean wasImplicitContactPoint() {
              return true;
            }
          };
        }
      };
    }
    return super.buildContext(configLoader, programmaticArguments);
  }

  /** Stops {@code adapterToStop} and logs any cleanup failure without masking the root error. */
  @VisibleForTesting
  static void stopAdapterQuietly(Adapter adapterToStop) {
    if (adapterToStop == null) {
      return;
    }
    try {
      adapterToStop.stop();
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to stop Adapter after CQL session build failure: {}", e.getMessage(), e);
    }
  }

  private void checkAdapterSettings() throws UnknownHostException {
    checkAndSetupHost();
    checkDatabaseUri();
    checkContactPoints();
    checkNumGrpcChannels();
    checkMaxCommitDelay();
  }

  private void checkAndSetupHost() {
    if (host == null) {
      final String env_var_endpoint = System.getenv(ENV_VAR_SPANNER_ENDPOINT);
      host = env_var_endpoint != null ? env_var_endpoint : DEFAULT_SPANNER_ENDPOINT;
    }
  }

  private void checkDatabaseUri() {
    if (databaseUri == null) {
      throw new NullPointerException("Spanner database URI must be set.");
    }
  }

  @VisibleForTesting
  void checkContactPoints() throws UnknownHostException {
    configContactPointProvided = false;
    // Restore original user contact-point state if a previous build registered an adapter
    // endpoint on this builder instance, ensuring each build gets a fresh ephemeral port.
    if (registeredAdapterEndPoint != null) {
      programmaticContactPoints.remove(registeredAdapterEndPoint);
      if (userContactPoint != null) {
        programmaticContactPoints.add(userContactPoint);
      }
      registeredAdapterEndPoint = null;
    }

    List<String> configContactPoints = loadConfigContactPoints();
    int configCount = configContactPoints != null ? configContactPoints.size() : 0;
    if (programmaticContactPoints.size() > 1 || configCount > 1) {
      throw new IllegalArgumentException(
          "At most one contact point can be provided when using SpannerCqlSessionBuilder, as it"
              + " connects to the local adapter.");
    }

    if (configCount == 1) {
      String configContactPoint = configContactPoints.get(0);
      HostAndPort parsed = parseConfigContactPoint(configContactPoint);
      InetAddress configAddress =
          resolveAndValidateSingleLoopbackAddress(parsed.getHost(), configContactPoint);

      if (programmaticContactPoints.size() == 1) {
        EndPoint providedEndPoint = programmaticContactPoints.iterator().next();
        InetSocketAddress programmaticAddress = validateAndExtractLoopbackAddress(providedEndPoint);
        if (programmaticAddress.getPort() != parsed.getPort()
            || !configAddress.equals(programmaticAddress.getAddress())) {
          throw new IllegalArgumentException(
              "At most one contact point can be provided when using SpannerCqlSessionBuilder, as"
                  + " it connects to the local adapter.");
        }
        iNetAddress = programmaticAddress.getAddress();
        port = programmaticAddress.getPort();
        userContactPoint = providedEndPoint;
        configContactPointProvided = true;
        LOG.debug("Using matching programmatic and config contact point: {}", programmaticAddress);
        return;
      }

      iNetAddress = configAddress;
      userContactPoint = null;
      configContactPointProvided = true;
      port = parsed.getPort();
      LOG.debug("Using config-provided localhost contact point: {}:{}", iNetAddress, port);
      return;
    }

    if (programmaticContactPoints.isEmpty()) {
      userContactPoint = null;
      iNetAddress = InetAddress.getLoopbackAddress();
      port = DEFAULT_PORT;
      LOG.info(
          "No contact point provided. Defaulting to connect adapter via {}:{}", iNetAddress, port);
      return;
    }

    // Exactly one programmatic contact point was provided by the user. Validate it.
    EndPoint providedEndPoint = programmaticContactPoints.iterator().next();
    InetSocketAddress inetSocketAddress = validateAndExtractLoopbackAddress(providedEndPoint);
    iNetAddress = inetSocketAddress.getAddress();
    userContactPoint = providedEndPoint;
    port = inetSocketAddress.getPort();
    LOG.debug("Using user-provided localhost contact point: {}", inetSocketAddress);
  }

  private List<String> loadConfigContactPoints() {
    ClassLoader classLoader = programmaticArgumentsBuilder.build().getClassLoader();
    try (DriverConfigLoader defaultLoader =
        configLoader == null ? defaultConfigLoader(classLoader) : null) {
      DriverConfigLoader effectiveLoader = configLoader != null ? configLoader : defaultLoader;
      if (effectiveLoader != null
          && effectiveLoader
              .getInitialConfig()
              .getDefaultProfile()
              .isDefined(DefaultDriverOption.CONTACT_POINTS)) {
        return effectiveLoader
            .getInitialConfig()
            .getDefaultProfile()
            .getStringList(DefaultDriverOption.CONTACT_POINTS);
      }
      return null;
    }
  }

  private static HostAndPort parseConfigContactPoint(String contactPoint) {
    HostAndPort parsed = HostAndPort.fromString(contactPoint);
    if (!parsed.hasPort() || parsed.getPort() <= 0 || Strings.isNullOrEmpty(parsed.getHost())) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader must specify host and a non-zero port, but got: "
              + contactPoint);
    }
    return parsed;
  }

  private static InetAddress resolveAndValidateSingleLoopbackAddress(
      String host, String contactPoint) {
    final InetAddress[] resolvedAddresses;
    try {
      resolvedAddresses = InetAddress.getAllByName(host);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(
          "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
              + " address, but could not resolve: "
              + contactPoint,
          e);
    }
    if (resolvedAddresses == null || resolvedAddresses.length == 0) {
      throw new IllegalArgumentException(
          "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
              + " address, but could not resolve: "
              + contactPoint);
    }
    for (InetAddress address : resolvedAddresses) {
      if (address == null || !address.isLoopbackAddress()) {
        throw new IllegalArgumentException(
            "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
                + " address, but got: "
                + address);
      }
    }
    if (resolvedAddresses.length != 1) {
      throw new IllegalArgumentException(
          "At most one contact point address can be provided when using"
              + " SpannerCqlSessionBuilder, but '"
              + contactPoint
              + "' resolved to "
              + resolvedAddresses.length
              + " addresses.");
    }
    return resolvedAddresses[0];
  }

  private static InetSocketAddress validateAndExtractLoopbackAddress(EndPoint providedEndPoint) {
    SocketAddress address = providedEndPoint.resolve();
    if (address == null) {
      throw new IllegalArgumentException("Resolved contact point address cannot be null.");
    }
    if (!(address instanceof InetSocketAddress)) {
      // Should not happen with typical Cassandra contact points.
      throw new IllegalArgumentException(
          "Unsupported contact point address type: " + address.getClass().getName());
    }
    InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
    InetAddress resolved = inetSocketAddress.getAddress();
    if (resolved == null || !resolved.isLoopbackAddress()) {
      throw new IllegalArgumentException(
          "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
              + " address, but got: "
              + inetSocketAddress);
    }
    return inetSocketAddress;
  }

  private void checkNumGrpcChannels() {
    if (numGrpcChannels <= 0) {
      throw new IllegalArgumentException("Number of gRPC channels should be greater than 0.");
    }
  }

  private void checkMaxCommitDelay() {
    if (maxCommitDelay.isPresent()
        && (maxCommitDelay.get().isNegative()
            || maxCommitDelay.get().toMillis() > LARGEST_MAX_COMMIT_DELAY_MILLIS)) {
      throw new IllegalArgumentException(
          "The max commit delay must be > 0 and < " + LARGEST_MAX_COMMIT_DELAY_MILLIS + "ms.");
    }
  }

  @VisibleForTesting
  void createAndStartAdapter() {

    if (instanceType == InstanceType.OMNI && !DatabaseName.isParsableFrom(databaseUri)) {
      databaseUri = DatabaseName.of(SPANNER_OMNI_ID, SPANNER_OMNI_ID, databaseUri).toString();
    }
    DatabaseName databaseName = DatabaseName.parse(databaseUri);
    OpenTelemetry openTelemetry =
        enableBuiltInMetrics
            ? builtInMetricsProvider.getOrCreateOpenTelemetry(
                databaseName.getProject(), databaseName.getInstance())
            : OpenTelemetry.noop();
    BuiltInMetricsRecorder metricsRecorder =
        new BuiltInMetricsRecorder(
            openTelemetry,
            builtInMetricsProvider.createDefaultAttributes(databaseName.getDatabase()));

    AdapterOptions adapterOptions =
        AdapterOptions.newBuilder()
            .spannerEndpoint(host)
            .tcpPort(port)
            .inetAddress(iNetAddress)
            .databaseUri(databaseUri)
            .numGrpcChannels(numGrpcChannels)
            .channelProvider(channelProvider)
            .credentials(credentials)
            .maxCommitDelay(maxCommitDelay.orElse(null))
            .metricsRecorder(metricsRecorder)
            .useVirtualThreads(useVirtualThreads)
            .usePlainText(usePlainText)
            .setInstanceType(instanceType)
            .useClientCert(clientCertPath, clientKeyPath)
            .build();
    adapter = new Adapter(adapterOptions);
    adapter.start();
    int boundPort = adapter.getBoundPort();
    if (!configContactPointProvided && (programmaticContactPoints.isEmpty() || port == 0)) {
      registeredAdapterEndPoint =
          new DefaultEndPoint(new InetSocketAddress(iNetAddress, boundPort));
      programmaticContactPoints.clear();
      programmaticContactPoints.add(registeredAdapterEndPoint);
    }
  }

  @VisibleForTesting
  InetAddress getAdapterInetAddress() {
    return iNetAddress;
  }

  @VisibleForTesting
  int getAdapterPort() {
    return port;
  }

  @VisibleForTesting
  Adapter getAdapter() {
    return adapter;
  }

  @VisibleForTesting
  Set<EndPoint> getProgrammaticContactPoints() {
    return programmaticContactPoints;
  }

  @VisibleForTesting
  EndPoint getUserContactPoint() {
    return userContactPoint;
  }

  @VisibleForTesting
  String getHost() {
    return host;
  }

  @VisibleForTesting
  InstanceType getInstanceType() {
    return instanceType;
  }
}

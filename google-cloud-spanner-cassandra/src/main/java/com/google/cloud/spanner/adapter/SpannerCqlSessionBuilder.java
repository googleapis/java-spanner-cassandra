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
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsProvider;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsRecorder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.spanner.adapter.v1.DatabaseName;
import io.opentelemetry.api.OpenTelemetry;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.LinkedHashSet;
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

    // After the Adapter starts, build the CQL session.
    return adapterStartFuture.thenCompose(
        v -> {
          LOG.info("Creating CQL session for database URI: {}", databaseUri);
          return super.buildAsync();
        });
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

    List<String> configContactPoints = null;
    try (DriverConfigLoader defaultLoader =
        configLoader == null
            ? defaultConfigLoader(programmaticArgumentsBuilder.build().getClassLoader())
            : null) {
      DriverConfigLoader effectiveLoader = configLoader != null ? configLoader : defaultLoader;
      if (effectiveLoader != null
          && effectiveLoader
              .getInitialConfig()
              .getDefaultProfile()
              .isDefined(DefaultDriverOption.CONTACT_POINTS)) {
        configContactPoints =
            effectiveLoader
                .getInitialConfig()
                .getDefaultProfile()
                .getStringList(DefaultDriverOption.CONTACT_POINTS);
      }
    }

    Set<InetSocketAddress> uniqueContactPoints = new LinkedHashSet<>();
    boolean hasConfigContactPoints = configContactPoints != null && !configContactPoints.isEmpty();
    if (hasConfigContactPoints) {
      for (String configContactPoint : configContactPoints) {
        uniqueContactPoints.add(parseConfigContactPoint(configContactPoint));
      }
    }

    for (EndPoint providedEndPoint : programmaticContactPoints) {
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
      InetAddress resolved;
      if (inetSocketAddress.isUnresolved()) {
        resolved =
            resolveLoopbackHost(inetSocketAddress.getHostString(), inetSocketAddress.toString());
      } else {
        resolved = inetSocketAddress.getAddress();
        if (resolved == null || !resolved.isLoopbackAddress()) {
          throw new IllegalArgumentException(
              "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
                  + " address, but got: "
                  + inetSocketAddress);
        }
      }
      uniqueContactPoints.add(new InetSocketAddress(resolved, inetSocketAddress.getPort()));
    }

    if (uniqueContactPoints.size() > 1) {
      throw new IllegalArgumentException(
          "At most one contact point can be provided when using SpannerCqlSessionBuilder, as it"
              + " connects to the local adapter.");
    }

    if (uniqueContactPoints.isEmpty()) {
      userContactPoint = null;
      iNetAddress = InetAddress.getLoopbackAddress();
      port = DEFAULT_PORT;
      LOG.info(
          "No contact point provided. Defaulting to connect adapter via {}:{}", iNetAddress, port);
      return;
    }

    InetSocketAddress singleContactPoint = uniqueContactPoints.iterator().next();
    iNetAddress = singleContactPoint.getAddress();
    port = singleContactPoint.getPort();
    configContactPointProvided = hasConfigContactPoints;
    userContactPoint =
        programmaticContactPoints.isEmpty() ? null : programmaticContactPoints.iterator().next();
    LOG.debug("Using localhost contact point: {}:{}", iNetAddress, port);
  }

  /**
   * Parses a {@code host:port} contact point string from {@link DriverConfigLoader} (accepting both
   * bracketed and unbracketed IPv6 literals to match the DataStax driver) and validates that all
   * resolved addresses are loopback with a port in {@code 1..65535}.
   */
  private static InetSocketAddress parseConfigContactPoint(String contactPoint) {
    if (Strings.isNullOrEmpty(contactPoint)) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader must specify host and a non-zero port, but got: "
              + contactPoint);
    }
    int lastColon = contactPoint.lastIndexOf(':');
    if (lastColon <= 0 || lastColon == contactPoint.length() - 1) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader must specify host and a non-zero port, but got: "
              + contactPoint);
    }
    String rawHost = contactPoint.substring(0, lastColon);
    String portStr = contactPoint.substring(lastColon + 1);
    String parsedHost;
    if (rawHost.startsWith("[")) {
      if (!rawHost.endsWith("]") || rawHost.length() <= 2) {
        throw new IllegalArgumentException(
            "Contact point in DriverConfigLoader has malformed IPv6 brackets: " + contactPoint);
      }
      parsedHost = rawHost.substring(1, rawHost.length() - 1);
    } else if (rawHost.endsWith("]")) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader has malformed IPv6 brackets: " + contactPoint);
    } else {
      parsedHost = rawHost;
    }
    if (Strings.isNullOrEmpty(parsedHost)) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader must specify host and a non-zero port, but got: "
              + contactPoint);
    }
    final int parsedPort;
    try {
      parsedPort = Integer.parseInt(portStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader must specify host and a non-zero port, but got: "
              + contactPoint,
          e);
    }
    if (parsedPort <= 0 || parsedPort > 65535) {
      throw new IllegalArgumentException(
          "Contact point in DriverConfigLoader must specify host and a non-zero port, but got: "
              + contactPoint);
    }
    InetAddress resolved = resolveLoopbackHost(parsedHost, contactPoint);
    return new InetSocketAddress(resolved, parsedPort);
  }

  /**
   * Resolves all IP addresses for {@code host}, verifies that every resolved address is a loopback
   * address, and returns the primary address for binding the embedded adapter.
   */
  private static InetAddress resolveLoopbackHost(String host, String displayValue) {
    final InetAddress[] resolvedAddresses;
    try {
      resolvedAddresses = InetAddress.getAllByName(host);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(
          "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
              + " address, but could not resolve: "
              + displayValue,
          e);
    }
    if (resolvedAddresses.length == 0) {
      throw new IllegalArgumentException(
          "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
              + " address, but could not resolve: "
              + displayValue);
    }
    for (InetAddress addr : resolvedAddresses) {
      if (addr == null || !addr.isLoopbackAddress()) {
        throw new IllegalArgumentException(
            "Contact point for in-process SpannerCqlSessionBuilder must be a resolved loopback"
                + " address, but got: "
                + addr);
      }
    }
    return resolvedAddresses[0];
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

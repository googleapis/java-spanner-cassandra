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

import com.google.cloud.spanner.adapter.configs.ConfigConstants;
import com.google.cloud.spanner.adapter.configs.ListenerConfigs;
import com.google.cloud.spanner.adapter.configs.UserConfigs;
import com.google.cloud.spanner.adapter.configs.YamlConfigLoader;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsProvider;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsRecorder;
import com.google.common.base.Strings;
import com.google.spanner.adapter.v1.DatabaseName;
import io.opentelemetry.api.OpenTelemetry;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for running a Spanner Cassandra Adapter as a stand-alone application.
 *
 * <p>The adapter can be configured using a YAML file, specified by the {@code
 * -DconfigFilePath=/path/to/config.yaml} system property. This is the recommended approach for
 * production and complex setups, as it supports multiple listeners and global settings.
 *
 * <p>For simpler setups or quick testing, configuration can be provided via system properties. This
 * method only supports a single adapter listener.
 *
 * <p><b>YAML Configuration Structure:</b>
 *
 * <pre>
 * globalClientConfigs:
 *   enableBuiltInMetrics: true
 *   healthCheckEndpoint: "127.0.0.1:8080"
 * listeners:
 *   - name: "listener_1"
 *     host: "127.0.0.1"
 *     port: 9042
 *     spanner:
 *       databaseUri: "projects/my-project/instances/my-instance/databases/my-database"
 *   - name: "listener_2"
 *     ...
 * </pre>
 *
 * <p><b>System Property Configuration (for a single listener):</b>
 *
 * <ul>
 *   <li>{@code databaseUri}: (Required) The URI of the target Spanner database.
 *   <li>{@code host}: (Optional) The hostname or IP address to bind the service to. Defaults to
 *       "0.0.0.0".
 *   <li>{@code port}: (Optional) The port number to bind the service to. Defaults to 9042.
 *   <li>{@code numGrpcChannels}: (Optional) The number of gRPC channels to use for communication
 *       with Spanner. Defaults to 4.
 *   <li>{@code maxCommitDelayMillis}: (Optional) The max commit delay to set in requests to
 *       optimize write throughput, in milliseconds. Defaults to none.
 *   <li>{@code healthCheckPort}: (Optional) The port number for the health check server. If
 *       unspecifed, health check server will NOT be started.
 * </ul>
 *
 * <p><b>Example Usage:</b>
 *
 * <p>Using a YAML configuration file:
 *
 * <pre>
 * java -DconfigFilePath=/path/to/config.yaml -jar path/to/your/spanner-cassandra-launcher.jar
 * </pre>
 *
 * <p>Using system properties for a single adapter:
 *
 * <pre>
 * java -DdatabaseUri=projects/my-project/instances/my-instance/databases/my-database \
 * -Dhost=127.0.0.1 \
 * -Dport=9042 \
 * -DnumGrpcChannels=4 \
 * -DmaxCommitDelayMillis=5 \
 * -DhealthCheckPort=8080 \
 * -jar path/to/your/spanner-cassandra-launcher.jar
 * </pre>
 *
 * @see Adapter
 */
public class Launcher {
  private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);
  private static final BuiltInMetricsProvider builtInMetricsProvider =
      BuiltInMetricsProvider.INSTANCE;
  private final AdapterFactory adapterFactory;
  private final List<Adapter> adapters = new ArrayList<>();
  private HealthCheckServer healthCheckServer;

  /**
   * Factory for creating Adapter and HealthCheckServer instances. This class allows for mocking
   * these dependencies in tests.
   */
  public static class AdapterFactory {
    public Adapter createAdapter(AdapterOptions options) {
      return new Adapter(options);
    }

    public HealthCheckServer createHealthCheckServer(InetAddress hostAddress, int port)
        throws IOException {
      return new HealthCheckServer(hostAddress, port);
    }
  }

  public Launcher() {
    this(new AdapterFactory());
  }

  public Launcher(AdapterFactory adapterFactory) {
    this.adapterFactory = adapterFactory;
  }

  public static void main(String[] args) throws Exception {
    Launcher launcher = new Launcher();

    Map<String, String> propertiesMap =
        System.getProperties().stringPropertyNames().stream()
            .collect(Collectors.toMap(Function.identity(), System.getProperties()::getProperty));
    launcher.run(propertiesMap);

    // Keep the main thread alive until all adapters are shut down.
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Parses the configuration, starts all configured listeners and the health check server, and
   * registers a shutdown hook for graceful termination.
   *
   * @param properties A map of configuration properties.
   * @throws IOException if there is an error reading the configuration file or starting the network
   *     servers.
   * @throws IllegalArgumentException if the configuration is invalid (e.g., missing required
   *     properties, invalid port numbers).
   * @throws IllegalStateException if one or more adapters fail to start.
   */
  public void run(Map<String, String> properties) throws Exception {
    final LauncherConfig config = parseConfiguration(properties);
    if (config.healthCheckConfig != null) {
      startHealthCheckServer(config.healthCheckConfig);
    } else {
      LOG.info("Health check server is disabled.");
    }

    final List<String> failedListeners = new ArrayList<>();
    for (ListenerConfig listenerConfig : config.listeners) {
      try {
        startAdapter(listenerConfig);
      } catch (Exception e) {
        String error = String.format("listener on port %d", listenerConfig.port);
        LOG.error("Failed to start adapter for {}: {}", error, e.getMessage());
        failedListeners.add(error);
      }
    }

    final boolean allAdaptersStarted = failedListeners.isEmpty();
    if (healthCheckServer != null) {
      healthCheckServer.setReady(allAdaptersStarted);
    }

    if (!allAdaptersStarted) {
      throw new IllegalStateException("One or more adapters failed to start: " + failedListeners);
    }

    // Register the single shutdown hook after all adapters are configured and started.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (healthCheckServer != null) {
                    healthCheckServer.stop();
                  }
                  adapters.forEach(
                      adapter -> {
                        try {
                          adapter.stop();
                        } catch (IOException e) {
                          LOG.warn("Error while stopping Adapter: " + e.getMessage());
                        }
                      });
                }));
  }

  private LauncherConfig parseConfiguration(Map<String, String> properties) throws IOException {
    final String configFilePath = properties.get(ConfigConstants.CONFIG_FILE_PROP_KEY);
    if (configFilePath != null) {
      LOG.info("Loading configuration from file: {}", configFilePath);
      try (InputStream inputStream = new FileInputStream(configFilePath)) {
        UserConfigs userConfigs = YamlConfigLoader.load(inputStream);
        return LauncherConfig.fromUserConfigs(userConfigs);
      } catch (FileNotFoundException e) {
        throw new IllegalArgumentException("Configuration file not found: " + configFilePath, e);
      }
    } else {
      LOG.info("Loading configuration from system properties.");
      return LauncherConfig.fromProperties(properties);
    }
  }

  private void startHealthCheckServer(HealthCheckConfig config) throws IOException {
    healthCheckServer = adapterFactory.createHealthCheckServer(config.hostAddress, config.port);
    healthCheckServer.start();
  }

  private AdapterOptions buildAdapterOptions(
      ListenerConfig config, BuiltInMetricsRecorder metricsRecorder) {
    final AdapterOptions.Builder opBuilder =
        new AdapterOptions.Builder()
            .spannerEndpoint(config.spannerEndpoint)
            .tcpPort(config.port)
            .databaseUri(config.databaseUri)
            .inetAddress(config.hostAddress)
            .numGrpcChannels(config.numGrpcChannels)
            .metricsRecorder(metricsRecorder);
    if (config.maxCommitDelayMillis != null) {
      opBuilder.maxCommitDelay(Duration.ofMillis(config.maxCommitDelayMillis));
    }
    return opBuilder.build();
  }

  private BuiltInMetricsRecorder createMetricsRecorder(
      boolean enableBuiltInMetrics, DatabaseName databaseName) {
    final OpenTelemetry openTelemetry =
        enableBuiltInMetrics
            ? builtInMetricsProvider.getOrCreateOpenTelemetry(
                databaseName.getProject(), databaseName.getInstance())
            : OpenTelemetry.noop();
    return new BuiltInMetricsRecorder(
        openTelemetry, builtInMetricsProvider.createDefaultAttributes(databaseName.getDatabase()));
  }

  private void startAdapter(ListenerConfig config) throws IOException {
    LOG.info(
        "Starting Adapter for Spanner database {} on {}:{} with {} gRPC channels, max commit"
            + " delay of {} and built-in metrics enabled: {}",
        config.databaseUri,
        config.hostAddress,
        config.port,
        config.numGrpcChannels,
        config.maxCommitDelayMillis,
        config.enableBuiltInMetrics);

    final DatabaseName databaseName = DatabaseName.parse(config.databaseUri);
    final BuiltInMetricsRecorder metricsRecorder =
        createMetricsRecorder(config.enableBuiltInMetrics, databaseName);
    final AdapterOptions options = buildAdapterOptions(config, metricsRecorder);

    final Adapter adapter = adapterFactory.createAdapter(options);
    adapters.add(adapter);
    adapter.start();
  }

  /** Encapsulates the full configuration for the Launcher. */
  private static final class LauncherConfig {
    private final List<ListenerConfig> listeners;
    @Nullable private final HealthCheckConfig healthCheckConfig;

    private LauncherConfig(
        List<ListenerConfig> listeners, @Nullable HealthCheckConfig healthCheckConfig) {
      this.listeners = listeners;
      this.healthCheckConfig = healthCheckConfig;
    }

    static LauncherConfig fromUserConfigs(UserConfigs userConfigs) throws UnknownHostException {
      if (userConfigs == null) {
        throw new IllegalArgumentException("UserConfigs cannot be null.");
      }
      if (userConfigs.getListeners() == null || userConfigs.getListeners().isEmpty()) {
        throw new IllegalArgumentException("No listeners defined in the configuration.");
      }

      final String globalSpannerEndpoint;
      final boolean globalEnableBuiltInMetrics;
      HealthCheckConfig healthCheckConfig = null;

      if (userConfigs.getGlobalClientConfigs() != null) {
        globalSpannerEndpoint =
            userConfigs.getGlobalClientConfigs().getSpannerEndpoint() != null
                ? userConfigs.getGlobalClientConfigs().getSpannerEndpoint()
                : ConfigConstants.DEFAULT_SPANNER_ENDPOINT;
        globalEnableBuiltInMetrics =
            userConfigs.getGlobalClientConfigs().getEnableBuiltInMetrics() != null
                && userConfigs.getGlobalClientConfigs().getEnableBuiltInMetrics();
        if (userConfigs.getGlobalClientConfigs().getHealthCheckEndpoint() != null) {
          healthCheckConfig =
              HealthCheckConfig.fromEndpointString(
                  userConfigs.getGlobalClientConfigs().getHealthCheckEndpoint());
        }
      } else {
        globalSpannerEndpoint = ConfigConstants.DEFAULT_SPANNER_ENDPOINT;
        globalEnableBuiltInMetrics = false;
      }

      List<ListenerConfig> listenerConfigs = new ArrayList<>();
      for (ListenerConfigs listener : userConfigs.getListeners()) {
        validateListenerConfig(listener);
        listenerConfigs.add(
            ListenerConfig.fromListenerConfigs(
                listener, globalSpannerEndpoint, globalEnableBuiltInMetrics));
      }

      return new LauncherConfig(listenerConfigs, healthCheckConfig);
    }

    static LauncherConfig fromProperties(Map<String, String> properties)
        throws UnknownHostException {
      String databaseUri = properties.get(ConfigConstants.DATABASE_URI_PROP_KEY);
      if (databaseUri == null) {
        throw new IllegalArgumentException(
            "Spanner database URI not set. Please set it using the '"
                + ConfigConstants.DATABASE_URI_PROP_KEY
                + "' property.");
      }

      ListenerConfig listenerConfig = ListenerConfig.fromProperties(properties);
      HealthCheckConfig healthCheckConfig = HealthCheckConfig.fromProperties(properties);

      return new LauncherConfig(Collections.singletonList(listenerConfig), healthCheckConfig);
    }

    private static void validateListenerConfig(ListenerConfigs listener) {
      if (listener.getSpanner() == null
          || Strings.isNullOrEmpty(listener.getSpanner().getDatabaseUri())) {
        throw new IllegalArgumentException(
            String.format(
                "Listener '%s' on port %s must have a non-empty 'spanner.databaseUri' defined.",
                listener.getName(), listener.getPort()));
      }
    }
  }

  /** Encapsulates the configuration for a single Adapter listener. */
  private static final class ListenerConfig {
    private final String databaseUri;
    private final InetAddress hostAddress;
    private final int port;
    private final String spannerEndpoint;
    private final int numGrpcChannels;
    @Nullable private final Integer maxCommitDelayMillis;
    private final boolean enableBuiltInMetrics;

    private ListenerConfig(
        String databaseUri,
        InetAddress hostAddress,
        int port,
        String spannerEndpoint,
        int numGrpcChannels,
        @Nullable Integer maxCommitDelayMillis,
        boolean enableBuiltInMetrics) {
      this.databaseUri = databaseUri;
      this.hostAddress = hostAddress;
      this.port = port;
      this.spannerEndpoint = spannerEndpoint;
      this.numGrpcChannels = numGrpcChannels;
      this.maxCommitDelayMillis = maxCommitDelayMillis;
      this.enableBuiltInMetrics = enableBuiltInMetrics;
    }

    static ListenerConfig fromListenerConfigs(
        ListenerConfigs listener, String globalSpannerEndpoint, boolean globalEnableBuiltInMetrics)
        throws UnknownHostException {
      String host = listener.getHost() != null ? listener.getHost() : ConfigConstants.DEFAULT_HOST;
      int port = listener.getPort() != null ? listener.getPort() : ConfigConstants.DEFAULT_PORT;
      int numGrpcChannels =
          listener.getSpanner().getNumGrpcChannels() != null
              ? listener.getSpanner().getNumGrpcChannels()
              : ConfigConstants.DEFAULT_NUM_GRPC_CHANNELS;
      Integer maxCommitDelayMillis = listener.getSpanner().getMaxCommitDelayMillis();

      return new ListenerConfig(
          listener.getSpanner().getDatabaseUri(),
          InetAddress.getByName(host),
          port,
          globalSpannerEndpoint,
          numGrpcChannels,
          maxCommitDelayMillis,
          globalEnableBuiltInMetrics);
    }

    static ListenerConfig fromProperties(Map<String, String> properties)
        throws UnknownHostException {
      String host =
          properties.getOrDefault(ConfigConstants.HOST_PROP_KEY, ConfigConstants.DEFAULT_HOST);
      int port =
          Integer.parseInt(
              properties.getOrDefault(
                  ConfigConstants.PORT_PROP_KEY, String.valueOf(ConfigConstants.DEFAULT_PORT)));
      int numGrpcChannels =
          Integer.parseInt(
              properties.getOrDefault(
                  ConfigConstants.NUM_GRPC_CHANNELS_PROP_KEY,
                  String.valueOf(ConfigConstants.DEFAULT_NUM_GRPC_CHANNELS)));
      String maxCommitDelayProperty = properties.get(ConfigConstants.MAX_COMMIT_DELAY_PROP_KEY);
      Integer maxCommitDelayMillis =
          maxCommitDelayProperty != null ? Integer.parseInt(maxCommitDelayProperty) : null;
      boolean enableBuiltInMetrics =
          Boolean.parseBoolean(
              properties.getOrDefault(ConfigConstants.ENABLE_BUILTIN_METRICS_PROP_KEY, "false"));

      return new ListenerConfig(
          properties.get(ConfigConstants.DATABASE_URI_PROP_KEY),
          InetAddress.getByName(host),
          port,
          ConfigConstants.DEFAULT_SPANNER_ENDPOINT,
          numGrpcChannels,
          maxCommitDelayMillis,
          enableBuiltInMetrics);
    }
  }

  /** Encapsulates the configuration for the health check server. */
  private static final class HealthCheckConfig {
    private final InetAddress hostAddress;
    private final int port;

    private HealthCheckConfig(InetAddress hostAddress, int port) {
      this.hostAddress = hostAddress;
      this.port = port;
    }

    static HealthCheckConfig fromEndpointString(String endpoint) throws UnknownHostException {
      String[] parts = endpoint.split(":");
      if (parts.length != 2) {
        throw new IllegalArgumentException(
            "Invalid health check endpoint format '" + endpoint + "'. Expected 'host:port'.");
      }
      String host = parts[0];
      int port = parsePort(parts[1]);
      return new HealthCheckConfig(InetAddress.getByName(host), port);
    }

    @Nullable
    static HealthCheckConfig fromProperties(Map<String, String> properties)
        throws UnknownHostException {
      String healthCheckPortStr = properties.get(ConfigConstants.HEALTH_CHECK_PORT_PROP_KEY);
      if (healthCheckPortStr == null) {
        return null;
      }
      String host =
          properties.getOrDefault(ConfigConstants.HOST_PROP_KEY, ConfigConstants.DEFAULT_HOST);
      int port = parsePort(healthCheckPortStr);
      return new HealthCheckConfig(InetAddress.getByName(host), port);
    }

    private static int parsePort(String portStr) {
      try {
        int port = Integer.parseInt(portStr);
        if (port < 0 || port > 65535) {
          throw new IllegalArgumentException(
              String.format("Invalid health check port '%s'. Must be between 0 and 65535", port));
        }
        return port;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("Invalid health check port '%s'. Must be a number.", portStr), e);
      }
    }
  }
}

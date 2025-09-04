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

import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsProvider;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsRecorder;
import com.google.spanner.adapter.v1.DatabaseName;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    final LauncherConfig config = LauncherConfigParser.parse(propertiesMap);
    launcher.run(config);

    // Keep the main thread alive until all adapters are shut down.
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      LOG.info("Main thread interrupted, shutting down.");
      launcher.shutdown();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Starts all configured listeners and the health check server, and registers a shutdown hook for
   * graceful termination.
   *
   * @param config The configuration for the launcher.
   * @throws IOException if there is an error starting the network servers.
   * @throws IllegalStateException if one or more adapters fail to start.
   */
  public void run(LauncherConfig config) throws Exception {
    if (config.getHealthCheckConfig() != null) {
      startHealthCheckServer(config.getHealthCheckConfig());
    } else {
      LOG.info("Health check server is disabled.");
    }

    final List<String> failedListeners = new ArrayList<>();
    for (ListenerConfig listenerConfig : config.getListeners()) {
      try {
        startAdapter(listenerConfig);
      } catch (Exception e) {
        String error = String.format("listener on port %d", listenerConfig.getPort());
        LOG.error("Failed to start adapter for {}: {}", error, e.getMessage());
        failedListeners.add(error);
      }
    }

    final boolean allAdaptersStarted = failedListeners.isEmpty();
    if (healthCheckServer != null) {
      healthCheckServer.setReady(allAdaptersStarted);
    }

    if (!allAdaptersStarted) {
      shutdown();
      throw new IllegalStateException("One or more adapters failed to start: " + failedListeners);
    }

    // Register the single shutdown hook after all adapters are configured and started.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutdown hook triggered. Stopping all adapters.");
                  shutdown();
                }));
  }

  /**
   * Stops all running adapters and the health check server. This method is automatically called by
   * a shutdown hook when the JVM terminates, but can also be called programmatically for a graceful
   * shutdown.
   */
  public void shutdown() {
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
  }

  private void startHealthCheckServer(HealthCheckConfig config) throws IOException {
    healthCheckServer =
        adapterFactory.createHealthCheckServer(config.getHostAddress(), config.getPort());
    healthCheckServer.start();
  }

  private AdapterOptions buildAdapterOptions(
      ListenerConfig config, BuiltInMetricsRecorder metricsRecorder) {
    final AdapterOptions.Builder opBuilder =
        new AdapterOptions.Builder()
            .spannerEndpoint(config.getSpannerEndpoint())
            .tcpPort(config.getPort())
            .databaseUri(config.getDatabaseUri())
            .inetAddress(config.getHostAddress())
            .numGrpcChannels(config.getNumGrpcChannels())
            .metricsRecorder(metricsRecorder);
    if (config.getMaxCommitDelayMillis() != null) {
      opBuilder.maxCommitDelay(Duration.ofMillis(config.getMaxCommitDelayMillis()));
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
        config.getDatabaseUri(),
        config.getHostAddress(),
        config.getPort(),
        config.getNumGrpcChannels(),
        config.getMaxCommitDelayMillis(),
        config.isEnableBuiltInMetrics());

    final DatabaseName databaseName = DatabaseName.parse(config.getDatabaseUri());
    final BuiltInMetricsRecorder metricsRecorder =
        createMetricsRecorder(config.isEnableBuiltInMetrics(), databaseName);
    final AdapterOptions options = buildAdapterOptions(config, metricsRecorder);

    final Adapter adapter = adapterFactory.createAdapter(options);
    adapters.add(adapter);
    adapter.start();
  }
}

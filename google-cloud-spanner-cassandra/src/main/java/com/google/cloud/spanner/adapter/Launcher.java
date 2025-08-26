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
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for running a Spanner Cassandra Adapter as a stand-alone application.
 *
 * <p>This class reads configuration parameters from system properties, initializes the underlying
 * {@link Adapter}, registers a shutdown hook for graceful termination, and starts the adapter
 * service.
 *
 * <p>Configuration is provided via the following system properties:
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
 * Example usage:
 *
 * <pre>
 * java -DdatabaseUri=projects/my-project/instances/my-instance/databases/my-database \
 * -Dhost=127.0.0.1 \
 * -Dport=9042 \
 * -DnumGrpcChannels=4 \
 * -DmaxCommitDelayMillis=5 \
 * -DhealthCheckPort=8080 \
 * -jar com.google.cloud.spanner.adapter.SpannerCassandraLauncher
 * </pre>
 *
 * @see Adapter
 */
public class Launcher {
  private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);
  private static final BuiltInMetricsProvider builtInMetricsProvider =
      BuiltInMetricsProvider.INSTANCE;
  private static final String DEFAULT_SPANNER_ENDPOINT = "spanner.googleapis.com:443";
  private static final String DATABASE_URI_PROP_KEY = "databaseUri";
  private static final String HOST_PROP_KEY = "host";
  private static final String PORT_PROP_KEY = "port";
  private static final String NUM_GRPC_CHANNELS_PROP_KEY = "numGrpcChannels";
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final String DEFAULT_PORT = "9042";
  private static final String DEFAULT_NUM_GRPC_CHANNELS = "4";
  private static final String MAX_COMMIT_DELAY_PROP_KEY = "maxCommitDelayMillis";
  private static final String ENABLE_BUILTIN_METRICS_PROP_KEY = "enableBuiltInMetrics";
  private static final String HEALTH_CHECK_PORT_PROP_KEY = "healthCheckPort";

  private final Adapter adapter;
  private final HealthCheckServer healthCheckServer;

  Launcher(Adapter adapter, @Nullable HealthCheckServer healthCheckServer) {
    this.adapter = adapter;
    this.healthCheckServer = healthCheckServer;
  }

  void launch() {
    if (healthCheckServer != null) {
      healthCheckServer.start();
    }
    adapter.start();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (healthCheckServer != null) {
                    healthCheckServer.stop();
                  }
                  try {
                    adapter.stop();
                  } catch (IOException e) {
                    LOG.warn("Error while stopping Adapter: " + e.getMessage());
                  }
                }));

    if (healthCheckServer != null) {
      healthCheckServer.setReady(true);
    }
  }

  public static void main(String[] args) throws Exception {
    final String databaseUri = System.getProperty(DATABASE_URI_PROP_KEY);
    final InetAddress inetAddress =
        InetAddress.getByName(System.getProperty(HOST_PROP_KEY, DEFAULT_HOST));
    final int port = Integer.parseInt(System.getProperty(PORT_PROP_KEY, DEFAULT_PORT));
    final int numGrpcChannels =
        Integer.parseInt(System.getProperty(NUM_GRPC_CHANNELS_PROP_KEY, DEFAULT_NUM_GRPC_CHANNELS));
    final String maxCommitDelayProperty = System.getProperty(MAX_COMMIT_DELAY_PROP_KEY);
    final boolean enableBuiltInMetrics =
        Boolean.parseBoolean(System.getProperty(ENABLE_BUILTIN_METRICS_PROP_KEY, "false"));
    final String healthCheckPortStr = System.getProperty(HEALTH_CHECK_PORT_PROP_KEY);
    HealthCheckServer healthCheckServer = null;

    if (databaseUri == null) {
      throw new IllegalArgumentException(
          "Spanner database URI not set. Please set it using -DdatabaseUri option.");
    }

    if (healthCheckPortStr != null) {
      final int healthCheckPort = Integer.parseInt(healthCheckPortStr);
      if (healthCheckPort < 0 || healthCheckPort > 65535) {
        throw new IllegalArgumentException(
            "Invalid health check port '" + healthCheckPort + "'. Must be between 0 and 65535");
      }
      healthCheckServer = new HealthCheckServer(inetAddress, healthCheckPort);
    } else {
      LOG.debug("Health check server is disabled.");
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

    AdapterOptions.Builder opBuilder =
        new AdapterOptions.Builder()
            .spannerEndpoint(DEFAULT_SPANNER_ENDPOINT)
            .tcpPort(port)
            .databaseUri(databaseUri)
            .inetAddress(inetAddress)
            .numGrpcChannels(numGrpcChannels)
            .metricsRecorder(metricsRecorder);
    if (maxCommitDelayProperty != null) {
      opBuilder.maxCommitDelay(Duration.ofMillis(Integer.parseInt(maxCommitDelayProperty)));
    }

    Adapter adapter = new Adapter(opBuilder.build());
    LOG.info(
        "Starting Adapter for Spanner database {} on {}:{} with {} gRPC channels, max commit"
            + " delay of {} and built-in metrics enabled: {}",
        databaseUri,
        inetAddress,
        port,
        numGrpcChannels,
        maxCommitDelayProperty,
        enableBuiltInMetrics);
    Launcher launcher = new Launcher(adapter, healthCheckServer);
    launcher.launch();

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

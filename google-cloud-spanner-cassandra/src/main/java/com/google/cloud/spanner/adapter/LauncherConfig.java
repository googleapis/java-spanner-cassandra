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
import com.google.common.base.Strings;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Encapsulates the full configuration for the Launcher. */
public final class LauncherConfig {
  private final List<ListenerConfig> listeners;
  @Nullable private final HealthCheckConfig healthCheckConfig;

  private LauncherConfig(
      List<ListenerConfig> listeners, @Nullable HealthCheckConfig healthCheckConfig) {
    this.listeners = listeners;
    this.healthCheckConfig = healthCheckConfig;
  }

  public List<ListenerConfig> getListeners() {
    return listeners;
  }

  @Nullable
  public HealthCheckConfig getHealthCheckConfig() {
    return healthCheckConfig;
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
    final boolean usePlainText;
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
      usePlainText =
          userConfigs.getGlobalClientConfigs().getUsePlainText() != null
              && userConfigs.getGlobalClientConfigs().getUsePlainText();
    } else {
      globalSpannerEndpoint = ConfigConstants.DEFAULT_SPANNER_ENDPOINT;
      globalEnableBuiltInMetrics = false;
      usePlainText = false;
    }

    List<ListenerConfig> listenerConfigs = new ArrayList<>();
    for (ListenerConfigs listener : userConfigs.getListeners()) {
      validateListenerConfig(listener);
      listenerConfigs.add(
          ListenerConfig.fromListenerConfigs(
              listener, globalSpannerEndpoint, globalEnableBuiltInMetrics, usePlainText));
    }

    return new LauncherConfig(listenerConfigs, healthCheckConfig);
  }

  static LauncherConfig fromProperties(Map<String, String> properties) throws UnknownHostException {
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
final class ListenerConfig {
  private final String databaseUri;
  private final InetAddress hostAddress;
  private final int port;
  private final String spannerEndpoint;
  private final int numGrpcChannels;
  @Nullable private final Integer maxCommitDelayMillis;
  private final boolean enableBuiltInMetrics;
  private final boolean usePlainText;

  private ListenerConfig(Builder builder) {
    this.databaseUri = builder.databaseUri;
    this.hostAddress = builder.hostAddress;
    this.port = builder.port;
    this.spannerEndpoint = builder.spannerEndpoint;
    this.numGrpcChannels = builder.numGrpcChannels;
    this.maxCommitDelayMillis = builder.maxCommitDelayMillis;
    this.enableBuiltInMetrics = builder.enableBuiltInMetrics;
    this.usePlainText = builder.usePlainText;
  }

  public String getDatabaseUri() {
    return databaseUri;
  }

  public InetAddress getHostAddress() {
    return hostAddress;
  }

  public int getPort() {
    return port;
  }

  public String getSpannerEndpoint() {
    return spannerEndpoint;
  }

  public int getNumGrpcChannels() {
    return numGrpcChannels;
  }

  @Nullable
  public Integer getMaxCommitDelayMillis() {
    return maxCommitDelayMillis;
  }

  public boolean isEnableBuiltInMetrics() {
    return enableBuiltInMetrics;
  }

  public boolean usePlainText() {
    return usePlainText;
  }

  static ListenerConfig fromListenerConfigs(
      ListenerConfigs listener,
      String globalSpannerEndpoint,
      boolean globalEnableBuiltInMetrics,
      boolean usePlainText)
      throws UnknownHostException {
    String host = listener.getHost() != null ? listener.getHost() : ConfigConstants.DEFAULT_HOST;
    int port = listener.getPort() != null ? listener.getPort() : ConfigConstants.DEFAULT_PORT;
    int numGrpcChannels =
        listener.getSpanner().getNumGrpcChannels() != null
            ? listener.getSpanner().getNumGrpcChannels()
            : ConfigConstants.DEFAULT_NUM_GRPC_CHANNELS;
    Integer maxCommitDelayMillis = listener.getSpanner().getMaxCommitDelayMillis();

    return newBuilder()
        .databaseUri(listener.getSpanner().getDatabaseUri())
        .hostAddress(InetAddress.getByName(host))
        .port(port)
        .spannerEndpoint(globalSpannerEndpoint)
        .numGrpcChannels(numGrpcChannels)
        .maxCommitDelayMillis(maxCommitDelayMillis)
        .enableBuiltInMetrics(globalEnableBuiltInMetrics)
        .usePlainText(usePlainText)
        .build();
  }

  static ListenerConfig fromProperties(Map<String, String> properties) throws UnknownHostException {
    String host =
        properties.getOrDefault(ConfigConstants.HOST_PROP_KEY, ConfigConstants.DEFAULT_HOST);
    int port =
        Integer.parseInt(
            properties.getOrDefault(
                ConfigConstants.PORT_PROP_KEY, String.valueOf(ConfigConstants.DEFAULT_PORT)));
    String spannerEndpoint =
        properties.getOrDefault(
            ConfigConstants.SPANNER_ENDPOINT_PROP_KEY, ConfigConstants.DEFAULT_SPANNER_ENDPOINT);
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
    boolean usePlainText =
        Boolean.parseBoolean(
            properties.getOrDefault(ConfigConstants.USE_PLAINTEXT_PROP_KEY, "false"));

    return newBuilder()
        .databaseUri(properties.get(ConfigConstants.DATABASE_URI_PROP_KEY))
        .hostAddress(InetAddress.getByName(host))
        .port(port)
        .spannerEndpoint(spannerEndpoint)
        .numGrpcChannels(numGrpcChannels)
        .maxCommitDelayMillis(maxCommitDelayMillis)
        .enableBuiltInMetrics(enableBuiltInMetrics)
        .usePlainText(usePlainText)
        .build();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {
    private String databaseUri;
    private InetAddress hostAddress;
    private int port;
    private String spannerEndpoint;
    private int numGrpcChannels;
    @Nullable private Integer maxCommitDelayMillis;
    private boolean enableBuiltInMetrics;
    private boolean usePlainText;

    public Builder databaseUri(String databaseUri) {
      this.databaseUri = databaseUri;
      return this;
    }

    public Builder hostAddress(InetAddress hostAddress) {
      this.hostAddress = hostAddress;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder spannerEndpoint(String spannerEndpoint) {
      this.spannerEndpoint = spannerEndpoint;
      return this;
    }

    public Builder numGrpcChannels(int numGrpcChannels) {
      this.numGrpcChannels = numGrpcChannels;
      return this;
    }

    public Builder maxCommitDelayMillis(@Nullable Integer maxCommitDelayMillis) {
      this.maxCommitDelayMillis = maxCommitDelayMillis;
      return this;
    }

    public Builder enableBuiltInMetrics(boolean enableBuiltInMetrics) {
      this.enableBuiltInMetrics = enableBuiltInMetrics;
      return this;
    }

    public Builder usePlainText(boolean usePlainText) {
      this.usePlainText = usePlainText;
      return this;
    }

    public ListenerConfig build() {
      return new ListenerConfig(this);
    }
  }
}

/** Encapsulates the configuration for the health check server. */
final class HealthCheckConfig {
  private final InetAddress hostAddress;
  private final int port;

  private HealthCheckConfig(Builder builder) {
    this.hostAddress = builder.hostAddress;
    this.port = builder.port;
  }

  public InetAddress getHostAddress() {
    return hostAddress;
  }

  public int getPort() {
    return port;
  }

  static HealthCheckConfig fromEndpointString(String endpoint) throws UnknownHostException {
    String[] parts = endpoint.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException(
          "Invalid health check endpoint format '" + endpoint + "'. Expected 'host:port'.");
    }
    String host = parts[0];
    int port = parsePort(parts[1]);
    return newBuilder().hostAddress(InetAddress.getByName(host)).port(port).build();
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
    return newBuilder().hostAddress(InetAddress.getByName(host)).port(port).build();
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

  static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {
    private InetAddress hostAddress;
    private int port;

    public Builder hostAddress(InetAddress hostAddress) {
      this.hostAddress = hostAddress;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public HealthCheckConfig build() {
      return new HealthCheckConfig(this);
    }
  }
}

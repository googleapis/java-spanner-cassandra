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

package com.google.cloud.spanner.adapter.configs;

import java.util.Map;

/** Represents the global client configurations loaded from a YAML file. */
public class GlobalClientConfigs {
  private final String spannerEndpoint;
  private final Boolean enableBuiltInMetrics;
  private final String healthCheckEndpoint;
  private final Boolean usePlainText;
  private final String experimentalHostEndpoint;
  private final String clientCertPath;
  private final String clientKeyPath;
  private final String proxyTLSCertPath;
  private final String proxyTLSKeyPath;

  private GlobalClientConfigs(Builder builder) {
    this.spannerEndpoint = builder.spannerEndpoint;
    this.enableBuiltInMetrics = builder.enableBuiltInMetrics;
    this.healthCheckEndpoint = builder.healthCheckEndpoint;
    this.usePlainText = builder.usePlainText;
    this.experimentalHostEndpoint = builder.experimentalHostEndpoint;
    this.clientCertPath = builder.clientCertPath;
    this.clientKeyPath = builder.clientKeyPath;
    this.proxyTLSCertPath = builder.proxyTLSCertPath;
    this.proxyTLSKeyPath = builder.proxyTLSKeyPath;
  }

  public GlobalClientConfigs(
      String spannerEndpoint, Boolean enableBuiltInMetrics, String healthCheckEndpoint) {
    this(
        new Builder()
            .spannerEndpoint(spannerEndpoint)
            .enableBuiltInMetrics(enableBuiltInMetrics)
            .healthCheckEndpoint(healthCheckEndpoint));
  }

  public GlobalClientConfigs(
      String spannerEndpoint,
      Boolean enableBuiltInMetrics,
      String healthCheckEndpoint,
      Boolean usePlainText) {
    this(
        new Builder()
            .spannerEndpoint(spannerEndpoint)
            .enableBuiltInMetrics(enableBuiltInMetrics)
            .healthCheckEndpoint(healthCheckEndpoint)
            .usePlainText(usePlainText));
  }

  public static class Builder {
    private String spannerEndpoint;
    private Boolean enableBuiltInMetrics;
    private String healthCheckEndpoint;
    private Boolean usePlainText;
    private String experimentalHostEndpoint;
    private String clientCertPath;
    private String clientKeyPath;
    private String proxyTLSCertPath;
    private String proxyTLSKeyPath;

    public Builder spannerEndpoint(String spannerEndpoint) {
      this.spannerEndpoint = spannerEndpoint;
      return this;
    }

    public Builder enableBuiltInMetrics(Boolean enableBuiltInMetrics) {
      this.enableBuiltInMetrics = enableBuiltInMetrics;
      return this;
    }

    public Builder healthCheckEndpoint(String healthCheckEndpoint) {
      this.healthCheckEndpoint = healthCheckEndpoint;
      return this;
    }

    public Builder usePlainText(Boolean usePlainText) {
      this.usePlainText = usePlainText;
      return this;
    }

    public Builder experimentalHostEndpoint(String experimentalHostEndpoint) {
      this.experimentalHostEndpoint = experimentalHostEndpoint;
      return this;
    }

    public Builder clientCertPath(String clientCertPath) {
      this.clientCertPath = clientCertPath;
      return this;
    }

    public Builder clientKeyPath(String clientKeyPath) {
      this.clientKeyPath = clientKeyPath;
      return this;
    }

    public Builder proxyTLSCertPath(String proxyTLSCertPath) {
      this.proxyTLSCertPath = proxyTLSCertPath;
      return this;
    }

    public Builder proxyTLSKeyPath(String proxyTLSKeyPath) {
      this.proxyTLSKeyPath = proxyTLSKeyPath;
      return this;
    }

    public GlobalClientConfigs build() {
      return new GlobalClientConfigs(this);
    }
  }

  public static GlobalClientConfigs fromMap(Map<String, Object> yamlMap) {
    String spannerEndpoint = (String) yamlMap.get("spannerEndpoint");
    Boolean enableBuiltInMetrics = (Boolean) yamlMap.get("enableBuiltInMetrics");
    String healthCheckEndpoint = (String) yamlMap.get("healthCheckEndpoint");
    Boolean usePlainText = (Boolean) yamlMap.get("usePlainText");
    String experimentalHostEndpoint = (String) yamlMap.get("experimentalHostEndpoint");
    String clientCertPath = (String) yamlMap.get("clientCertPath");
    String clientKeyPath = (String) yamlMap.get("clientKeyPath");
    String proxyTLSCertPath = (String) yamlMap.get("proxyTLSCertPath");
    String proxyTLSKeyPath = (String) yamlMap.get("proxyTLSKeyPath");

    return new GlobalClientConfigs.Builder()
        .spannerEndpoint(spannerEndpoint)
        .enableBuiltInMetrics(enableBuiltInMetrics)
        .healthCheckEndpoint(healthCheckEndpoint)
        .usePlainText(usePlainText)
        .experimentalHostEndpoint(experimentalHostEndpoint)
        .clientCertPath(clientCertPath)
        .clientKeyPath(clientKeyPath)
        .proxyTLSCertPath(proxyTLSCertPath)
        .proxyTLSKeyPath(proxyTLSKeyPath)
        .build();
  }

  public String getSpannerEndpoint() {
    return spannerEndpoint;
  }

  public Boolean getEnableBuiltInMetrics() {
    return enableBuiltInMetrics;
  }

  public String getHealthCheckEndpoint() {
    return healthCheckEndpoint;
  }

  public Boolean getUsePlainText() {
    return usePlainText;
  }

  public String getExperimentalHostEndpoint() {
    return experimentalHostEndpoint;
  }

  public String getClientCertPath() {
    return clientCertPath;
  }

  public String getClientKeyPath() {
    return clientKeyPath;
  }

  public String getProxyTLSCertPath() {
    return proxyTLSCertPath;
  }

  public String getProxyTLSKeyPath() {
    return proxyTLSKeyPath;
  }
}

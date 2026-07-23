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

import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder.InstanceType;
import com.google.common.base.Strings;
import java.util.Map;

/** Represents the global client configurations loaded from a YAML file. */
public class GlobalClientConfigs {
  private final String spannerEndpoint;
  private final Boolean enableBuiltInMetrics;
  private final String healthCheckEndpoint;
  private final Boolean usePlainText;
  private final InstanceType instanceType;
  private final String clientCertPath;
  private final String clientKeyPath;

  public GlobalClientConfigs(
      String spannerEndpoint,
      Boolean enableBuiltInMetrics,
      String healthCheckEndpoint,
      Boolean usePlainText,
      String experimentalHostEndpoint,
      InstanceType instanceType,
      String clientCertPath,
      String clientKeyPath) {
    if (!Strings.isNullOrEmpty(experimentalHostEndpoint)) {
      if (Strings.isNullOrEmpty(spannerEndpoint)) {
        spannerEndpoint = experimentalHostEndpoint;
      }
      if (instanceType == null) {
        instanceType = InstanceType.OMNI;
      }
    }
    this.spannerEndpoint = spannerEndpoint;
    this.enableBuiltInMetrics = enableBuiltInMetrics;
    this.healthCheckEndpoint = healthCheckEndpoint;
    this.usePlainText = usePlainText;
    this.instanceType = instanceType;
    this.clientCertPath = clientCertPath;
    this.clientKeyPath = clientKeyPath;
  }

  /**
   * @deprecated Use {@link #GlobalClientConfigs(String, Boolean, String, Boolean, String,
   *     InstanceType, String, String)} instead.
   */
  @Deprecated
  public GlobalClientConfigs(
      String spannerEndpoint,
      Boolean enableBuiltInMetrics,
      String healthCheckEndpoint,
      Boolean usePlainText,
      String experimentalHostEndpoint,
      String instanceType,
      String clientCertPath,
      String clientKeyPath) {
    this(
        spannerEndpoint,
        enableBuiltInMetrics,
        healthCheckEndpoint,
        usePlainText,
        experimentalHostEndpoint,
        Strings.isNullOrEmpty(instanceType)
            ? null
            : InstanceType.valueOf(instanceType.toUpperCase()),
        clientCertPath,
        clientKeyPath);
  }

  public GlobalClientConfigs(
      String spannerEndpoint, Boolean enableBuiltInMetrics, String healthCheckEndpoint) {
    this(
        spannerEndpoint,
        enableBuiltInMetrics,
        healthCheckEndpoint,
        null,
        null,
        (InstanceType) null,
        null,
        null);
  }

  public GlobalClientConfigs(
      String spannerEndpoint,
      Boolean enableBuiltInMetrics,
      String healthCheckEndpoint,
      Boolean usePlainText) {
    this(
        spannerEndpoint,
        enableBuiltInMetrics,
        healthCheckEndpoint,
        usePlainText,
        null,
        (InstanceType) null,
        null,
        null);
  }

  /**
   * @deprecated Use {@link #GlobalClientConfigs(String, Boolean, String, Boolean, String,
   *     InstanceType, String, String)} instead.
   */
  @Deprecated
  public GlobalClientConfigs(
      String spannerEndpoint,
      Boolean enableBuiltInMetrics,
      String healthCheckEndpoint,
      Boolean usePlainText,
      String experimentalHostEndpoint) {
    this(
        spannerEndpoint,
        enableBuiltInMetrics,
        healthCheckEndpoint,
        usePlainText,
        experimentalHostEndpoint,
        (InstanceType) null,
        null,
        null);
  }

  public static GlobalClientConfigs fromMap(Map<String, Object> yamlMap) {
    String spannerEndpoint = (String) yamlMap.get("spannerEndpoint");
    Boolean enableBuiltInMetrics = (Boolean) yamlMap.get("enableBuiltInMetrics");
    String healthCheckEndpoint = (String) yamlMap.get("healthCheckEndpoint");
    Boolean usePlainText = (Boolean) yamlMap.get("usePlainText");
    String experimentalHostEndpoint = (String) yamlMap.get("experimentalHostEndpoint");
    String instanceTypeStr = (String) yamlMap.get("instanceType");
    InstanceType instanceType =
        Strings.isNullOrEmpty(instanceTypeStr)
            ? null
            : InstanceType.valueOf(instanceTypeStr.toUpperCase());
    String clientCertPath = (String) yamlMap.get("clientCertPath");
    String clientKeyPath = (String) yamlMap.get("clientKeyPath");

    if (Strings.isNullOrEmpty(clientCertPath) || Strings.isNullOrEmpty(clientKeyPath)) {
      return new GlobalClientConfigs(
          spannerEndpoint,
          enableBuiltInMetrics,
          healthCheckEndpoint,
          usePlainText,
          experimentalHostEndpoint,
          instanceType,
          null,
          null);
    }
    return new GlobalClientConfigs(
        spannerEndpoint,
        enableBuiltInMetrics,
        healthCheckEndpoint,
        usePlainText,
        experimentalHostEndpoint,
        instanceType,
        clientCertPath,
        clientKeyPath);
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

  /**
   * @deprecated Use {@link #getSpannerEndpoint()} and {@link #getInstanceType()} instead.
   */
  @Deprecated
  public String getExperimentalHostEndpoint() {
    return instanceType == InstanceType.OMNI ? spannerEndpoint : null;
  }

  public InstanceType getInstanceType() {
    return instanceType;
  }

  public String getClientCertPath() {
    return clientCertPath;
  }

  public String getClientKeyPath() {
    return clientKeyPath;
  }
}

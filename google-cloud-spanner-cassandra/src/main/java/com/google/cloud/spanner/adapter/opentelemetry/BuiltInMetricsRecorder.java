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
package com.google.cloud.spanner.adapter.opentelemetry;

import com.google.api.gax.core.GaxProperties;
import com.google.common.base.Preconditions;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;

/** Implementation for recording built in metrics. */
public class BuiltInMetricsRecorder {

  private final LongCounter operationCountRecorder;
  private final DoubleHistogram operationLatencyRecorder;
  private final Map<String, String> clientAttr;

  /**
   * Creates the following instruments for the following metrics:
   *
   * <ul>
   *   <li>Operation Latency: Histogram
   *   <li>Operation Count: Counter
   * </ul>
   *
   * @param openTelemetry OpenTelemetry instance
   * @param serviceName Service Name
   * @param clientAttr Client attibutes
   */
  BuiltInMetricsRecorder(
      OpenTelemetry openTelemetry, String serviceName, Map<String, String> clientAttr) {
    Meter meter =
        openTelemetry
            .meterBuilder(BuiltInMetricsConstant.SPANNER_METER_NAME)
            .setInstrumentationVersion(GaxProperties.getLibraryVersion(getClass()))
            .build();

    this.operationLatencyRecorder =
        meter
            .histogramBuilder(serviceName + "/" + BuiltInMetricsConstant.OPERATION_LATENCIES_NAME)
            .setDescription("Total time until final operation success or failure.")
            .setUnit("ms")
            .build();

    this.operationCountRecorder =
        meter
            .counterBuilder(serviceName + "/" + BuiltInMetricsConstant.OPERATION_COUNT_NAME)
            .setDescription("Number of Operations")
            .setUnit("1")
            .build();
    this.clientAttr = clientAttr;
  }

  public void recordOperationLatency(double operationLatency, Map<String, String> attributes) {
    operationLatencyRecorder.record(operationLatency, toOtelAttributes(attributes));
  }

  public void recordOperationCount(long count, Map<String, String> attributes) {
    operationCountRecorder.add(count, toOtelAttributes(attributes));
  }

  Attributes toOtelAttributes(Map<String, String> attributes) {
    Preconditions.checkNotNull(attributes, "Attributes map cannot be null");
    AttributesBuilder attributesBuilder = Attributes.builder();
    attributes.forEach(attributesBuilder::put);
    this.clientAttr.forEach(attributesBuilder::put);
    return attributesBuilder.build();
  }
}

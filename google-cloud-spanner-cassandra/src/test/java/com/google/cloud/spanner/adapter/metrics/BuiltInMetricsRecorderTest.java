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
package com.google.cloud.spanner.adapter.metrics;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuiltInMetricsRecorderTest {

  private OpenTelemetry mockOpenTelemetry;
  private Meter mockMeter; // This is the Meter instance built by the MeterBuilder
  private DoubleHistogram mockLatencyRecorder;
  private LongCounter mockOperationCounter;

  private BuiltInMetricsRecorder metricsRecorder;
  private Attributes defaultAttributes;

  @Before
  public void setUp() {
    // 1. Initialize Mocks
    mockOpenTelemetry = mock(OpenTelemetry.class); // Mock the OpenTelemetry API
    MeterBuilder mockMeterBuilder = mock(MeterBuilder.class);
    mockMeter = mock(Meter.class);
    DoubleHistogramBuilder mockHistogramBuilder = mock(DoubleHistogramBuilder.class);
    mockLatencyRecorder = mock(DoubleHistogram.class);
    LongCounterBuilder mockCounterBuilder = mock(LongCounterBuilder.class);
    mockOperationCounter = mock(LongCounter.class);

    // 2. Stub the mock chain for OpenTelemetry -> Meter
    when(mockOpenTelemetry.meterBuilder(anyString())).thenReturn(mockMeterBuilder);
    when(mockMeterBuilder.setInstrumentationVersion(anyString())).thenReturn(mockMeterBuilder);
    when(mockMeterBuilder.build()).thenReturn(mockMeter);

    // 3. Stub the mock chain for Meter -> DoubleHistogram (Latency Recorder)
    when(mockMeter.histogramBuilder(anyString())).thenReturn(mockHistogramBuilder);
    when(mockHistogramBuilder.setDescription(anyString())).thenReturn(mockHistogramBuilder);
    when(mockHistogramBuilder.setUnit(anyString())).thenReturn(mockHistogramBuilder);
    when(mockHistogramBuilder.build()).thenReturn(mockLatencyRecorder);

    // 4. Stub the mock chain for Meter -> LongCounter (Operation Counter)
    when(mockMeter.counterBuilder(anyString())).thenReturn(mockCounterBuilder);
    when(mockCounterBuilder.setDescription(anyString())).thenReturn(mockCounterBuilder);
    when(mockCounterBuilder.setUnit(anyString())).thenReturn(mockCounterBuilder);
    when(mockCounterBuilder.build()).thenReturn(mockOperationCounter);

    // 5. Setup default attributes and create the recorder instance
    defaultAttributes =
        Attributes.builder()
            .put("db", "test-db")
            .put("client_id", "test-client")
            .put("method", "myMethod")
            .put("status", "OK")
            .build();
    metricsRecorder = new BuiltInMetricsRecorder(mockOpenTelemetry, defaultAttributes);
  }

  @Test
  public void recordOperationLatency_callsRecordWithCorrectValues() {
    double latency = 123.45;

    metricsRecorder.recordOperationLatency(latency);

    Attributes expectedAttributes =
        Attributes.builder()
            .put("db", "test-db")
            .put("client_id", "test-client")
            .put("method", "myMethod")
            .put("status", "OK")
            .build();
    verify(mockLatencyRecorder).record(latency, expectedAttributes);
  }

  @Test
  public void recordOperationCount_callsAddWithCorrectValues() {
    long count = 5L;
    metricsRecorder.recordOperationCount(count);

    Attributes expectedAttributes =
        Attributes.builder()
            .put("db", "test-db")
            .put("client_id", "test-client")
            .put("method", "myMethod")
            .put("status", "OK")
            .build();
    verify(mockOperationCounter).add(count, expectedAttributes);
  }
}

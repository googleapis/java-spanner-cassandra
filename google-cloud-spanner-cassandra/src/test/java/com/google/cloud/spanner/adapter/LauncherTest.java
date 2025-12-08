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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.adapter.configs.GlobalClientConfigs;
import com.google.cloud.spanner.adapter.configs.ListenerConfigs;
import com.google.cloud.spanner.adapter.configs.SpannerConfigs;
import com.google.cloud.spanner.adapter.configs.UserConfigs;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LauncherTest {

  private static final String DEFAULT_DATABASE_URI = "projects/p/instances/i/databases/d";

  @Mock private Launcher.AdapterFactory mockAdapterFactory;
  @Mock private Adapter mockAdapter;
  @Mock private HealthCheckServer mockHealthCheckServer;
  @Captor private ArgumentCaptor<Thread> shutdownHookCaptor;
  @Captor private ArgumentCaptor<AdapterOptions> adapterOptionsCaptor;

  private Launcher launcher;
  private PrintStream originalSystemOut;

  @Before
  public void setUp() throws IOException {
    launcher = new Launcher(mockAdapterFactory);
    when(mockAdapterFactory.createAdapter(any())).thenReturn(mockAdapter);
    when(mockAdapterFactory.createHealthCheckServer(any(), anyInt()))
        .thenReturn(mockHealthCheckServer);

    // Redirect System.out to avoid console output during test
    originalSystemOut = System.out;
    System.setOut(new PrintStream(new ByteArrayOutputStream()));
  }

  @After
  public void tearDown() {
    System.setOut(originalSystemOut);
  }

  @Test
  public void testRun_withMultipleListeners_startsMultipleAdapters() throws Exception {
    UserConfigs userConfigs =
        new UserConfigs(
            new GlobalClientConfigs("spanner.googleapis.com:443", true, "127.0.0.1:8080"),
            Arrays.asList(
                new ListenerConfigs(
                    "listener_1",
                    "127.0.0.1",
                    9042,
                    new SpannerConfigs("projects/p/instances/i/databases/d-1-config-test", 4, 5)),
                new ListenerConfigs(
                    "listener_2",
                    "0.0.0.0",
                    9043,
                    new SpannerConfigs(
                        "projects/p/instances/i/databases/d-2-config-test", 8, null))));
    LauncherConfig config = LauncherConfig.fromUserConfigs(userConfigs);

    launcher.run(config);

    verify(mockAdapterFactory, times(2)).createAdapter(adapterOptionsCaptor.capture());
    verify(mockAdapterFactory, times(1)).createHealthCheckServer(any(), eq(8080));
    verify(mockAdapter, times(2)).start();
    verify(mockHealthCheckServer).start();
    verify(mockHealthCheckServer).setReady(true);

    AdapterOptions options1 = adapterOptionsCaptor.getAllValues().get(0);
    assertThat(options1.getDatabaseUri())
        .isEqualTo("projects/p/instances/i/databases/d-1-config-test");
    assertThat(options1.getTcpPort()).isEqualTo(9042);
    assertThat(options1.getInetAddress()).isEqualTo(InetAddress.getByName("127.0.0.1"));
    assertThat(options1.getSpannerEndpoint()).isEqualTo("spanner.googleapis.com:443");
    assertThat(options1.usePlainText()).isFalse();

    AdapterOptions options2 = adapterOptionsCaptor.getAllValues().get(1);
    assertThat(options2.getDatabaseUri())
        .isEqualTo("projects/p/instances/i/databases/d-2-config-test");
    assertThat(options2.getTcpPort()).isEqualTo(9043);
    assertThat(options2.getInetAddress()).isEqualTo(InetAddress.getByName("0.0.0.0"));
    assertThat(options2.getSpannerEndpoint()).isEqualTo("spanner.googleapis.com:443");
    assertThat(options2.usePlainText()).isFalse();
  }

  @Test
  public void testRun_withSingleListener_startsAdapterWithOptions() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("host", "127.0.0.1");
    properties.put("port", "9042");
    properties.put("numGrpcChannels", "8");
    properties.put("maxCommitDelayMillis", "100");
    properties.put("enableBuiltInMetrics", "true");
    properties.put("healthCheckPort", "8080");
    LauncherConfig config = LauncherConfig.fromProperties(properties);

    launcher.run(config);

    verify(mockAdapterFactory, times(1)).createAdapter(adapterOptionsCaptor.capture());
    verify(mockAdapterFactory, times(1)).createHealthCheckServer(any(), eq(8080));
    verify(mockAdapter, times(1)).start();
    verify(mockHealthCheckServer).start();
    verify(mockHealthCheckServer).setReady(true);

    AdapterOptions options = adapterOptionsCaptor.getValue();
    assertThat(options.getDatabaseUri()).isEqualTo(DEFAULT_DATABASE_URI);
    assertThat(options.getTcpPort()).isEqualTo(9042);
    assertThat(options.getInetAddress()).isEqualTo(InetAddress.getByName("127.0.0.1"));
    assertThat(options.getNumGrpcChannels()).isEqualTo(8);
    assertThat(options.getMaxCommitDelay().get().toMillis()).isEqualTo(100);
    assertThat(options.getSpannerEndpoint()).isEqualTo("spanner.googleapis.com:443");
    assertThat(options.usePlainText()).isFalse();
  }

  @Test
  public void testRun_withUsePlainTextMode_startsAdapterWithOptions() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("host", "127.0.0.1");
    properties.put("port", "9042");
    properties.put("numGrpcChannels", "8");
    properties.put("maxCommitDelayMillis", "100");
    properties.put("enableBuiltInMetrics", "true");
    properties.put("healthCheckPort", "8080");
    properties.put("spannerEndpoint", "localhost:15000");
    properties.put("usePlainText", "true");
    LauncherConfig config = LauncherConfig.fromProperties(properties);

    launcher.run(config);

    verify(mockAdapterFactory, times(1)).createAdapter(adapterOptionsCaptor.capture());
    verify(mockAdapterFactory, times(1)).createHealthCheckServer(any(), eq(8080));
    verify(mockAdapter, times(1)).start();
    verify(mockHealthCheckServer).start();
    verify(mockHealthCheckServer).setReady(true);

    AdapterOptions options = adapterOptionsCaptor.getValue();
    assertThat(options.getDatabaseUri()).isEqualTo(DEFAULT_DATABASE_URI);
    assertThat(options.getTcpPort()).isEqualTo(9042);
    assertThat(options.getInetAddress()).isEqualTo(InetAddress.getByName("127.0.0.1"));
    assertThat(options.getNumGrpcChannels()).isEqualTo(8);
    assertThat(options.getMaxCommitDelay().get().toMillis()).isEqualTo(100);
    assertThat(options.getSpannerEndpoint()).isEqualTo("localhost:15000");
    assertThat(options.usePlainText()).isTrue();
  }

  @Test
  public void testRun_withNoHealthCheckPort_noHealthCheckServerIsCreated() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    LauncherConfig config = LauncherConfig.fromProperties(properties);

    launcher.run(config);

    verify(mockAdapterFactory, times(1)).createAdapter(any(AdapterOptions.class));
    verify(mockAdapter, times(1)).start();
    verify(mockAdapterFactory, never()).createHealthCheckServer(any(), anyInt());
    verify(mockHealthCheckServer, never()).start();
    verify(mockHealthCheckServer, never()).setReady(anyBoolean());
  }

  @Test
  public void testRun_whenAdapterStartFails_healthCheckIsNotReady() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("healthCheckPort", "8080");
    LauncherConfig config = LauncherConfig.fromProperties(properties);
    doThrow(new RuntimeException("Failed to start adapter")).when(mockAdapter).start();

    assertThrows(IllegalStateException.class, () -> launcher.run(config));
    verify(mockHealthCheckServer).start();
    verify(mockHealthCheckServer).setReady(false);
    // Verify that the shutdown logic was called to clean up.
    verify(mockHealthCheckServer, times(1)).stop();
    verify(mockAdapter, times(1)).stop();
  }

  @Test
  public void testShutdownHook_stopsAllInstances() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("healthCheckPort", "8080");
    LauncherConfig config = LauncherConfig.fromProperties(properties);

    launcher.run(config);
    launcher.shutdown();

    verify(mockAdapter, times(1)).stop();
    verify(mockHealthCheckServer, times(1)).stop();
  }
}

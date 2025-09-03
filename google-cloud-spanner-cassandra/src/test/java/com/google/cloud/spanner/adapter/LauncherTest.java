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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.adapter.configs.GlobalClientConfigs;
import com.google.cloud.spanner.adapter.configs.ListenerConfigs;
import com.google.cloud.spanner.adapter.configs.SpannerConfigs;
import com.google.cloud.spanner.adapter.configs.UserConfigs;
import com.google.cloud.spanner.adapter.configs.YamlConfigLoader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LauncherTest {

  private static final String DEFAULT_DATABASE_URI = "projects/p/instances/i/databases/d";
  private static final String CONFIG_FILE_PROP_KEY = "configFilePath";

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
  public void testRun_withValidConfigFile_startsMultipleAdapters() throws Exception {
    String configFilePath = "any/path/to/config.yaml";
    Map<String, String> properties = new HashMap<>();
    properties.put(CONFIG_FILE_PROP_KEY, configFilePath);

    UserConfigs userConfigs =
        new UserConfigs(
            new GlobalClientConfigs("spanner.googleapis.com:443", true, "127.0.0.1:8080"),
            Arrays.asList(
                new ListenerConfigs(
                    "listener_1",
                    "127.0.0.1",
                    9042,
                    new SpannerConfigs("projects/p/instances/i/databases/d-1-config-test", 4, 100)),
                new ListenerConfigs(
                    "listener_2",
                    "0.0.0.0",
                    9043,
                    new SpannerConfigs(
                        "projects/p/instances/i/databases/d-2-config-test", 8, null))));

    try (MockedStatic<Runtime> mockedRuntime = mockStatic(Runtime.class);
        MockedStatic<YamlConfigLoader> mockedLoader = mockStatic(YamlConfigLoader.class);
        MockedConstruction<FileInputStream> unused = mockConstruction(FileInputStream.class)) {
      mockedRuntime.when(Runtime::getRuntime).thenReturn(mock(Runtime.class));
      mockedLoader
          .when(() -> YamlConfigLoader.load(any(InputStream.class)))
          .thenReturn(userConfigs);
      launcher.run(properties);
    }

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

    AdapterOptions options2 = adapterOptionsCaptor.getAllValues().get(1);
    assertThat(options2.getDatabaseUri())
        .isEqualTo("projects/p/instances/i/databases/d-2-config-test");
    assertThat(options2.getTcpPort()).isEqualTo(9043);
    assertThat(options2.getInetAddress()).isEqualTo(InetAddress.getByName("0.0.0.0"));
  }

  @Test
  public void testRun_withSystemProperties_startsAdapterWithOptions() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("host", "127.0.0.1");
    properties.put("port", "9042");
    properties.put("numGrpcChannels", "8");
    properties.put("maxCommitDelayMillis", "100");
    properties.put("enableBuiltInMetrics", "true");
    properties.put("healthCheckPort", "8080");

    try (MockedStatic<Runtime> mockedRuntime = mockStatic(Runtime.class)) {
      mockedRuntime.when(Runtime::getRuntime).thenReturn(mock(Runtime.class));
      launcher.run(properties);
    }

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
  }

  @Test
  public void testRun_withNoHealthCheckPort_noHealthCheckServerIsCreated() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);

    try (MockedStatic<Runtime> mockedRuntime = mockStatic(Runtime.class)) {
      mockedRuntime.when(Runtime::getRuntime).thenReturn(mock(Runtime.class));
      launcher.run(properties);
    }

    verify(mockAdapterFactory, times(1)).createAdapter(any(AdapterOptions.class));
    verify(mockAdapter, times(1)).start();
    verify(mockAdapterFactory, never()).createHealthCheckServer(any(), anyInt());
    verify(mockHealthCheckServer, never()).start();
    verify(mockHealthCheckServer, never()).setReady(anyBoolean());
  }

  @Test
  public void testRun_withMissingDatabaseUri_throwsIllegalArgumentException() {
    Map<String, String> properties = Collections.emptyMap();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> launcher.run(properties));
    assertThat(thrown.getMessage()).contains("Spanner database URI not set.");
  }

  @Test
  public void testRun_withInvalidHealthCheckPort_throwsIllegalArgumentException() {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("healthCheckPort", "99999");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> launcher.run(properties));
    assertThat(thrown.getMessage()).contains("Invalid health check port '99999'");
  }

  @Test
  public void testRun_withInvalidHealthCheckEndpointInConfigFile_throwsIllegalArgumentException() {
    String configFilePath = "any/path/to/config.yaml";
    Map<String, String> properties = new HashMap<>();
    properties.put(CONFIG_FILE_PROP_KEY, configFilePath);

    try (MockedStatic<YamlConfigLoader> mockedLoader = mockStatic(YamlConfigLoader.class);
        MockedConstruction<FileInputStream> unused = mockConstruction(FileInputStream.class)) {
      mockedLoader
          .when(() -> YamlConfigLoader.load(any(InputStream.class)))
          .thenReturn(new UserConfigs(null, null));
      IllegalArgumentException thrown =
          assertThrows(IllegalArgumentException.class, () -> launcher.run(properties));
      assertThat(thrown.getMessage()).contains("No listeners defined in the configuration.");
    }
  }

  @Test
  public void testRun_whenAdapterStartFails_healthCheckIsNotReady() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("healthCheckPort", "8080");
    doThrow(new RuntimeException("Failed to start adapter")).when(mockAdapter).start();

    assertThrows(IllegalStateException.class, () -> launcher.run(properties));
    verify(mockHealthCheckServer).start();
    verify(mockHealthCheckServer).setReady(false);
  }

  @Test
  public void testShutdownHook_stopsAllInstances() throws Exception {
    when(mockAdapterFactory.createAdapter(any())).thenReturn(mockAdapter);
    when(mockAdapterFactory.createHealthCheckServer(any(), anyInt()))
        .thenReturn(mockHealthCheckServer);

    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("healthCheckPort", "8080");

    try (MockedStatic<Runtime> mockedRuntime = mockStatic(Runtime.class)) {
      Runtime mockRuntime = mock(Runtime.class);
      mockedRuntime.when(Runtime::getRuntime).thenReturn(mockRuntime);

      launcher.run(properties);

      verify(mockRuntime).addShutdownHook(shutdownHookCaptor.capture());
      shutdownHookCaptor.getValue().run();

      verify(mockAdapter, times(1)).stop();
      verify(mockHealthCheckServer, times(1)).stop();
    }
  }
}

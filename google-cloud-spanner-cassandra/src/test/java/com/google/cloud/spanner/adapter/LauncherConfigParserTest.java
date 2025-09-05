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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.adapter.configs.GlobalClientConfigs;
import com.google.cloud.spanner.adapter.configs.ListenerConfigs;
import com.google.cloud.spanner.adapter.configs.SpannerConfigs;
import com.google.cloud.spanner.adapter.configs.UserConfigs;
import com.google.cloud.spanner.adapter.configs.YamlConfigLoader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LauncherConfigParserTest {

  private static final String DEFAULT_DATABASE_URI = "projects/p/instances/i/databases/d";

  @Test
  public void testParse_withValidConfigFile() throws Exception {
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

    try (MockedStatic<YamlConfigLoader> mockedLoader = mockStatic(YamlConfigLoader.class);
        MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class)) {
      InetAddress mockAddress = mock(InetAddress.class);
      mockedInetAddress
          .when(() -> InetAddress.getByName(any(String.class)))
          .thenReturn(mockAddress);
      mockedLoader
          .when(() -> YamlConfigLoader.load(any(InputStream.class)))
          .thenReturn(userConfigs);

      LauncherConfig config = LauncherConfigParser.parse(mock(InputStream.class));

      assertThat(config.getListeners()).hasSize(2);
      assertThat(config.getHealthCheckConfig()).isNotNull();
    }
  }

  @Test
  public void testParse_withConfigFileAndOtherParams_usesConfigFile() throws Exception {
    String configFile = getClass().getClassLoader().getResource("valid-config.yaml").getFile();
    Map<String, String> properties = new HashMap<>();
    properties.put("configFilePath", configFile);
    // The following properties should be ignored, as the config file takes precedence.
    properties.put("databaseUri", "projects/p/instances/i/databases/d-from-props");
    properties.put("port", "9044");

    LauncherConfig config = LauncherConfigParser.parse(properties);

    assertThat(config.getListeners()).hasSize(2);
    ListenerConfig listenerConfig1 = config.getListeners().get(0);
    assertThat(listenerConfig1.getDatabaseUri())
        .isEqualTo("projects/my-project/instances/my-instance/databases/my-database");
    assertThat(listenerConfig1.getPort()).isEqualTo(9042);

    ListenerConfig listenerConfig2 = config.getListeners().get(1);
    assertThat(listenerConfig2.getDatabaseUri())
        .isEqualTo("projects/my-project/instances/my-instance/databases/my-database-2");
    assertThat(listenerConfig2.getPort()).isEqualTo(9043);
  }

  @Test
  public void testParse_withSystemProperties() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("host", "127.0.0.1");
    properties.put("port", "9042");
    properties.put("numGrpcChannels", "8");
    properties.put("maxCommitDelayMillis", "100");
    properties.put("enableBuiltInMetrics", "true");
    properties.put("healthCheckPort", "8080");

    try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class)) {
      InetAddress mockAddress = mock(InetAddress.class);
      when(mockAddress.getHostAddress()).thenReturn("127.0.0.1");
      mockedInetAddress.when(() -> InetAddress.getByName("127.0.0.1")).thenReturn(mockAddress);

      LauncherConfig config = LauncherConfigParser.parse(properties);
      assertThat(config.getListeners()).hasSize(1);
      ListenerConfig listenerConfig = config.getListeners().get(0);
      assertThat(listenerConfig.getDatabaseUri()).isEqualTo(DEFAULT_DATABASE_URI);
      assertThat(listenerConfig.getPort()).isEqualTo(9042);
      assertThat(listenerConfig.getHostAddress().getHostAddress()).isEqualTo("127.0.0.1");
      assertThat(listenerConfig.getNumGrpcChannels()).isEqualTo(8);
      assertThat(listenerConfig.getMaxCommitDelayMillis()).isEqualTo(100);
      assertThat(listenerConfig.isEnableBuiltInMetrics()).isTrue();
      assertThat(config.getHealthCheckConfig()).isNotNull();
      assertThat(config.getHealthCheckConfig().getPort()).isEqualTo(8080);
    }
  }

  @Test
  public void testParse_withMissingDatabaseUri_throwsIllegalArgumentException() {
    Map<String, String> properties = Collections.emptyMap();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> LauncherConfigParser.parse(properties));
    assertThat(thrown.getMessage()).contains("Spanner database URI not set.");
  }

  @Test
  public void testParse_withInvalidHealthCheckPort_throwsIllegalArgumentException() {
    Map<String, String> properties = new HashMap<>();
    properties.put("databaseUri", DEFAULT_DATABASE_URI);
    properties.put("healthCheckPort", "99999");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> LauncherConfigParser.parse(properties));
    assertThat(thrown.getMessage()).contains("Invalid health check port '99999'");
  }

  @Test
  public void testParse_withInvalidConfigFile_throwsIOException() {
    try (MockedStatic<YamlConfigLoader> mockedLoader = mockStatic(YamlConfigLoader.class)) {
      mockedLoader
          .when(() -> YamlConfigLoader.load(any(InputStream.class)))
          .thenReturn(new UserConfigs(null, null));
      IOException thrown =
          assertThrows(
              IOException.class, () -> LauncherConfigParser.parse(mock(InputStream.class)));
      assertThat(thrown.getCause()).isInstanceOf(IllegalArgumentException.class);
      assertThat(thrown.getCause().getMessage())
          .contains("No listeners defined in the configuration.");
    }
  }

  @Test
  public void testParse_withUnknownHost_throwsIOException() throws IOException {
    UserConfigs userConfigs =
        new UserConfigs(
            new GlobalClientConfigs("spanner.googleapis.com:443", true, "unknown-host:8080"),
            Collections.singletonList(
                new ListenerConfigs(
                    "listener_1",
                    "unknown-host",
                    9042,
                    new SpannerConfigs(DEFAULT_DATABASE_URI, 4, 5))));
    try (MockedStatic<YamlConfigLoader> mockedLoader = mockStatic(YamlConfigLoader.class)) {
      mockedLoader
          .when(() -> YamlConfigLoader.load(any(InputStream.class)))
          .thenReturn(userConfigs);

      IOException thrown =
          assertThrows(
              IOException.class, () -> LauncherConfigParser.parse(mock(InputStream.class)));
      assertThat(thrown.getCause()).isInstanceOf(UnknownHostException.class);
    }
  }
}

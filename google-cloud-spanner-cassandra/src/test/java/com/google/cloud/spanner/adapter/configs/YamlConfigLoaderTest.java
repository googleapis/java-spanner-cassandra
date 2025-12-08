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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.Test;
import org.yaml.snakeyaml.error.YAMLException;

public class YamlConfigLoaderTest {

  @Test
  public void testLoad_validYamlFile_parsesCorrectly() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("valid-config.yaml")) {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);

      assertThat(userConfigs).isNotNull();
      assertThat(userConfigs.getGlobalClientConfigs()).isNotNull();
      assertThat(userConfigs.getGlobalClientConfigs().getSpannerEndpoint())
          .isEqualTo("spanner.googleapis.com:443");
      assertThat(userConfigs.getGlobalClientConfigs().getEnableBuiltInMetrics()).isTrue();
      assertThat(userConfigs.getGlobalClientConfigs().getHealthCheckEndpoint())
          .isEqualTo("127.0.0.1:8080");
      assertThat(userConfigs.getGlobalClientConfigs().getUsePlainText()).isNull();

      List<ListenerConfigs> listeners = userConfigs.getListeners();
      assertThat(listeners).isNotNull();
      assertThat(listeners).hasSize(2);

      // Verify listener_1
      ListenerConfigs listener1 = listeners.get(0);
      assertThat(listener1.getName()).isEqualTo("listener_1");

      assertThat(listener1.getHost()).isEqualTo("127.0.0.1");
      assertThat(listener1.getPort()).isEqualTo(9042);

      assertThat(listener1.getSpanner()).isNotNull();
      assertThat(listener1.getSpanner().getDatabaseUri())
          .isEqualTo("projects/my-project/instances/my-instance/databases/my-database");

      assertThat(listener1.getSpanner().getNumGrpcChannels()).isEqualTo(4);

      assertThat(listener1.getSpanner().getMaxCommitDelayMillis()).isEqualTo(100);

      // Verify listener_2
      ListenerConfigs listener2 = listeners.get(1);
      assertThat(listener2.getName()).isEqualTo("listener_2");

      assertThat(listener2.getHost()).isEqualTo("127.0.0.2");
      assertThat(listener2.getPort()).isEqualTo(9043);

      assertThat(listener2.getSpanner()).isNotNull();
      assertThat(listener2.getSpanner().getDatabaseUri())
          .isEqualTo("projects/my-project/instances/my-instance/databases/my-database-2");

      assertThat(listener2.getSpanner().getNumGrpcChannels()).isEqualTo(8);

      assertThat(listener2.getSpanner().getMaxCommitDelayMillis()).isNull();
    }
  }

  @Test
  public void testLoad_validUsePlainTextYamlFile_parsesCorrectly() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("valid-useplaintext-config.yaml")) {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);

      assertThat(userConfigs).isNotNull();
      assertThat(userConfigs.getGlobalClientConfigs()).isNotNull();
      assertThat(userConfigs.getGlobalClientConfigs().getSpannerEndpoint())
          .isEqualTo("localhost:15000");
      assertThat(userConfigs.getGlobalClientConfigs().getEnableBuiltInMetrics()).isTrue();
      assertThat(userConfigs.getGlobalClientConfigs().getHealthCheckEndpoint())
          .isEqualTo("127.0.0.1:8080");
      assertThat(userConfigs.getGlobalClientConfigs().getUsePlainText()).isTrue();

      List<ListenerConfigs> listeners = userConfigs.getListeners();
      assertThat(listeners).isNotNull();
      assertThat(listeners).hasSize(2);
    }
  }

  @Test
  public void testLoad_emptyYaml_returnsNull() throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("empty.yaml")) {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);
      assertThat(userConfigs).isNull();
    }
  }

  @Test
  public void testLoad_malformedYaml_throwsException() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("malformed-config.yaml")) {
      assertThrows(YAMLException.class, () -> YamlConfigLoader.load(inputStream));
    }
  }

  @Test
  public void testLoad_incorrectDataType_throwsException() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("incorrect-datatype-config.yaml")) {
      assertThrows(ClassCastException.class, () -> YamlConfigLoader.load(inputStream));
    }
  }

  @Test
  public void testLoad_missingOptionalFields_parsesCorrectly() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("missing-optional-fields-config.yaml")) {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);

      assertThat(userConfigs).isNotNull();
      assertThat(userConfigs.getGlobalClientConfigs()).isNull();

      List<ListenerConfigs> listeners = userConfigs.getListeners();
      assertThat(listeners).isNotNull();
      assertThat(listeners).hasSize(1);

      ListenerConfigs listener = listeners.get(0);
      assertThat(listener).isNotNull();
      assertThat(listener.getSpanner()).isNotNull();
      assertThat(listener.getSpanner().getDatabaseUri()).isEqualTo("test");
      assertThat(listener.getPort()).isNull();
      assertThat(listener.getSpanner().getNumGrpcChannels()).isNull();
      assertThat(listener.getSpanner().getMaxCommitDelayMillis()).isNull();
    }
  }

  @Test
  public void testLoad_emptyListenersList_parsesCorrectly() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("empty-listeners.yaml")) {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);

      assertThat(userConfigs).isNotNull();
      assertThat(userConfigs.getListeners()).isNotNull();
      assertThat(userConfigs.getListeners()).isEmpty();
    }
  }

  @Test
  public void testLoad_missingListenersKey_parsesCorrectly() throws IOException {
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("missing-listeners-key.yaml")) {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);

      assertThat(userConfigs).isNotNull();
      assertThat(userConfigs.getListeners()).isNull();
      assertThat(userConfigs.getGlobalClientConfigs()).isNotNull();
    }
  }
}

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

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * A utility class for loading and parsing YAML configuration files into a {@link UserConfigs}
 * object.
 */
public class YamlConfigLoader {

  /**
   * Reads a YAML configuration from the provided {@link InputStream} and maps it to a {@link
   * UserConfigs} object.
   *
   * @param inputStream The input stream containing the YAML data.
   * @return A {@link UserConfigs} object representing the parsed configuration.
   */
  @Nullable
  public static UserConfigs load(InputStream inputStream) {
    Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
    Map<String, Object> yamlMap = yaml.load(inputStream);
    if (yamlMap == null) {
      return null;
    }
    return fromMap(yamlMap);
  }

  private static UserConfigs fromMap(Map<String, Object> yamlMap) {
    if (yamlMap == null) {
      return new UserConfigs(null, null);
    }
    GlobalClientConfigs globalClientConfigs = null;
    if (yamlMap.containsKey("globalClientConfigs")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> globalClientConfigsMap =
          (Map<String, Object>) yamlMap.get("globalClientConfigs");
      globalClientConfigs = GlobalClientConfigs.fromMap(globalClientConfigsMap);
    }

    List<ListenerConfigs> listeners = null;
    if (yamlMap.containsKey("listeners")) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> listenersListMap =
          (List<Map<String, Object>>) yamlMap.get("listeners");
      listeners =
          listenersListMap.stream().map(ListenerConfigs::fromMap).collect(Collectors.toList());
    }

    return new UserConfigs(globalClientConfigs, listeners);
  }
}

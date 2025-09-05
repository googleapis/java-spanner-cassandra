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
import com.google.cloud.spanner.adapter.configs.UserConfigs;
import com.google.cloud.spanner.adapter.configs.YamlConfigLoader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parses the configuration for the {@link Launcher}. */
public class LauncherConfigParser {
  private static final Logger LOG = LoggerFactory.getLogger(LauncherConfigParser.class);

  /**
   * Parses the configuration from the given properties.
   *
   * @param properties The properties to parse.
   * @return The parsed {@link LauncherConfig}.
   * @throws IOException If the configuration file cannot be read.
   */
  public static LauncherConfig parse(Map<String, String> properties) throws IOException {
    final String configFilePath = properties.get(ConfigConstants.CONFIG_FILE_PROP_KEY);
    if (configFilePath != null) {
      LOG.info("Loading configuration from file: {}", configFilePath);
      try (InputStream inputStream = new FileInputStream(configFilePath)) {
        return parse(inputStream);
      } catch (FileNotFoundException e) {
        throw new IllegalArgumentException("Configuration file not found: " + configFilePath, e);
      }
    } else {
      LOG.info("Loading configuration from system properties.");
      return LauncherConfig.fromProperties(properties);
    }
  }

  static LauncherConfig parse(InputStream inputStream) throws IOException {
    try {
      UserConfigs userConfigs = YamlConfigLoader.load(inputStream);
      return LauncherConfig.fromUserConfigs(userConfigs);
    } catch (Exception e) {
      throw new IOException("Failed to parse configuration from input stream", e);
    }
  }
}

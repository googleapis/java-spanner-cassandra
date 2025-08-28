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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the top-level user configuration object for the Spanner Cassandra Adapter, loaded from
 * a YAML file. This class contains global client settings and a list of specific listener
 * configurations.
 */
public class UserConfigs {
  private final ClientConfigs clientConfigs;
  private final List<ListenerConfigs> listeners;

  public UserConfigs(ClientConfigs clientConfigs, List<ListenerConfigs> listeners) {
    this.clientConfigs = clientConfigs;
    this.listeners = listeners;
  }

  public static UserConfigs fromMap(Map<String, Object> yamlMap) {
    if (yamlMap == null) {
      return new UserConfigs(null, null);
    }
    ClientConfigs clientConfigs = null;
    if (yamlMap.containsKey("clientConfigs")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> clientConfigsMap = (Map<String, Object>) yamlMap.get("clientConfigs");
      clientConfigs = ClientConfigs.fromMap(clientConfigsMap);
    }

    List<ListenerConfigs> listeners = null;
    if (yamlMap.containsKey("listeners")) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> listenersListMap =
          (List<Map<String, Object>>) yamlMap.get("listeners");
      listeners =
          listenersListMap.stream().map(ListenerConfigs::fromMap).collect(Collectors.toList());
    }

    return new UserConfigs(clientConfigs, listeners);
  }

  public ClientConfigs getClientConfigs() {
    return clientConfigs;
  }

  public List<ListenerConfigs> getListeners() {
    return listeners;
  }
}

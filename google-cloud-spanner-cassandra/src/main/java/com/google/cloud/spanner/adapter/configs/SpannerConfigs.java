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

import java.util.Map;

/**
 * Represents the Spanner client configurations, including details about sessions and operations,
 * loaded from a YAML file.
 */
public class SpannerConfigs {
  private final String databaseUri;
  private final SessionConfigs session;
  private final OperationConfigs operation;

  public SpannerConfigs(String databaseUri, SessionConfigs session, OperationConfigs operation) {
    this.databaseUri = databaseUri;
    this.session = session;
    this.operation = operation;
  }

  public static SpannerConfigs fromMap(Map<String, Object> yamlMap) {
    String databaseUri = (String) yamlMap.get("databaseUri");

    SessionConfigs session = null;
    if (yamlMap.containsKey("session")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> sessionMap = (Map<String, Object>) yamlMap.get("session");
      session = SessionConfigs.fromMap(sessionMap);
    }

    OperationConfigs operation = null;
    if (yamlMap.containsKey("operation")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> operationMap = (Map<String, Object>) yamlMap.get("operation");
      operation = OperationConfigs.fromMap(operationMap);
    }

    return new SpannerConfigs(databaseUri, session, operation);
  }

  public String getDatabaseUri() {
    return databaseUri;
  }

  public SessionConfigs getSession() {
    return session;
  }

  public OperationConfigs getOperation() {
    return operation;
  }
}

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

/** Centralized constants for configuration keys and default values. */
public final class ConfigConstants {

  // Private constructor to prevent instantiation
  private ConfigConstants() {}

  public static final String DEFAULT_SPANNER_ENDPOINT = "spanner.googleapis.com:443";
  public static final String SPANNER_ENDPOINT_PROP_KEY = "spannerEndpoint";
  public static final String DATABASE_URI_PROP_KEY = "databaseUri";
  public static final String HOST_PROP_KEY = "host";
  public static final String PORT_PROP_KEY = "port";
  public static final String NUM_GRPC_CHANNELS_PROP_KEY = "numGrpcChannels";
  public static final String DEFAULT_HOST = "0.0.0.0";
  public static final int DEFAULT_PORT = 9042;
  public static final int DEFAULT_NUM_GRPC_CHANNELS = 4;
  public static final String MAX_COMMIT_DELAY_PROP_KEY = "maxCommitDelayMillis";
  public static final String ENABLE_BUILTIN_METRICS_PROP_KEY = "enableBuiltInMetrics";
  public static final String HEALTH_CHECK_PORT_PROP_KEY = "healthCheckPort";
  public static final String CONFIG_FILE_PROP_KEY = "configFilePath";
  public static final String USE_PLAINTEXT_PROP_KEY = "usePlainText";
}

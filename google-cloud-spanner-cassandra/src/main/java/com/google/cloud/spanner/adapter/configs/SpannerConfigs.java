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

import static com.google.cloud.spanner.adapter.configs.ConfigConstants.DATABASE_URI_PROP_KEY;
import static com.google.cloud.spanner.adapter.configs.ConfigConstants.MAX_COMMIT_DELAY_PROP_KEY;
import static com.google.cloud.spanner.adapter.configs.ConfigConstants.NUM_GRPC_CHANNELS_PROP_KEY;

import java.util.Map;

/**
 * Represents the Spanner client configurations, including the database URI, the number of gRPC
 * channels, and the maximum commit delay. This object is loaded from a YAML file.
 */
public class SpannerConfigs {
  private final String databaseUri;
  private final Integer numGrpcChannels;
  private final Integer maxCommitDelayMillis;

  public SpannerConfigs(String databaseUri, Integer numGrpcChannels, Integer maxCommitDelayMillis) {
    this.databaseUri = databaseUri;
    this.numGrpcChannels = numGrpcChannels;
    this.maxCommitDelayMillis = maxCommitDelayMillis;
  }

  public static SpannerConfigs fromMap(Map<String, Object> yamlMap) {
    String databaseUri = (String) yamlMap.get(DATABASE_URI_PROP_KEY);
    Integer numGrpcChannels = (Integer) yamlMap.get(NUM_GRPC_CHANNELS_PROP_KEY);
    Integer maxCommitDelayMillis = (Integer) yamlMap.get(MAX_COMMIT_DELAY_PROP_KEY);

    return new SpannerConfigs(databaseUri, numGrpcChannels, maxCommitDelayMillis);
  }

  public String getDatabaseUri() {
    return databaseUri;
  }

  public Integer getNumGrpcChannels() {
    return numGrpcChannels;
  }

  public Integer getMaxCommitDelayMillis() {
    return maxCommitDelayMillis;
  }
}

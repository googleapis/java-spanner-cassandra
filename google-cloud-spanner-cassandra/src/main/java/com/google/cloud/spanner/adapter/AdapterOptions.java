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

import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsRecorder;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Optional;

/** Options for creating the {@link Adapter}. */
class AdapterOptions {

  private static final String DEFAULT_SPANNER_ENDPOINT = "spanner.googleapis.com:443";
  private static final int DEFAULT_NUM_GRPC_CHANNELS = 4;

  /** Builder class for creating an instance of {@link AdapterOptions}. */
  static class Builder {
    String spannerEndpoint = DEFAULT_SPANNER_ENDPOINT;
    int tcpPort;
    InetAddress inetAddress;
    String databaseUri;
    int numGrpcChannels = DEFAULT_NUM_GRPC_CHANNELS;
    Optional<Duration> maxCommitDelay = Optional.empty();
    private TransportChannelProvider channelProvider = null;
    private Credentials credentials;
    private BuiltInMetricsRecorder metricsRecorder;
    private boolean useVirtualThreads = false;

    /** The Cloud Spanner endpoint. */
    Builder spannerEndpoint(String spannerEndpoint) {
      this.spannerEndpoint = spannerEndpoint;
      return this;
    }

    /** The local TCP port number that the adapter server should listen on. */
    Builder tcpPort(int tcpPort) {
      this.tcpPort = tcpPort;
      return this;
    }

    /** The specific local {@link InetAddress} for the server socket to bind to. */
    Builder inetAddress(InetAddress inetAddress) {
      this.inetAddress = inetAddress;
      return this;
    }

    /** The URI of the Cloud Spanner database to connect to. */
    Builder databaseUri(String databaseUri) {
      this.databaseUri = databaseUri;
      return this;
    }

    /** (Optional) The number of gRPC channels to use for connections to Cloud Spanner. */
    Builder numGrpcChannels(int numGrpcChannels) {
      this.numGrpcChannels = numGrpcChannels;
      return this;
    }

    /** (Optional) The max commit delay to set in requests to optimize write throughput. */
    Builder maxCommitDelay(Duration maxCommitDelay) {
      this.maxCommitDelay = Optional.ofNullable(maxCommitDelay);
      return this;
    }

    /**
     * (Optional) The gRPC channel provider. If set to null or not specified, one will be created by
     * default.
     */
    Builder channelProvider(TransportChannelProvider channelProvider) {
      this.channelProvider = channelProvider;
      return this;
    }

    /**
     * (Optional) The Google Cloud credentials for accessing Cloud Spanner. If set to null or not
     * specified, the application default credentials will be used.
     */
    Builder credentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    Builder metricsRecorder(BuiltInMetricsRecorder metricsRecorder) {
      this.metricsRecorder = metricsRecorder;
      return this;
    }

    /** (Optional) Whether to use virtual threads (Java 21+ only) */
    Builder useVirtualThreads(boolean useVirtualThreads) {
      this.useVirtualThreads = useVirtualThreads;
      return this;
    }

    AdapterOptions build() {
      return new AdapterOptions(this);
    }
  }

  private final String spannerEndpoint;
  private final int tcpPort;
  private final InetAddress inetAddress;
  private final String databaseUri;
  private final int numGrpcChannels;
  private final Optional<Duration> maxCommitDelay;
  private TransportChannelProvider channelProvider;
  private Credentials credentials;
  private BuiltInMetricsRecorder metricsRecorder;
  private boolean useVirtualThreads;

  private AdapterOptions(Builder builder) {
    this.spannerEndpoint = builder.spannerEndpoint;
    this.tcpPort = builder.tcpPort;
    this.inetAddress = builder.inetAddress;
    this.databaseUri = builder.databaseUri;
    this.numGrpcChannels = builder.numGrpcChannels;
    this.maxCommitDelay = builder.maxCommitDelay;
    this.channelProvider = builder.channelProvider;
    this.credentials = builder.credentials;
    this.metricsRecorder = builder.metricsRecorder;
    this.useVirtualThreads = builder.useVirtualThreads;
  }

  static Builder newBuilder() {
    return new Builder();
  }

  String getSpannerEndpoint() {
    return spannerEndpoint;
  }

  int getTcpPort() {
    return tcpPort;
  }

  String getDatabaseUri() {
    return databaseUri;
  }

  InetAddress getInetAddress() {
    return inetAddress;
  }

  int getNumGrpcChannels() {
    return numGrpcChannels;
  }

  TransportChannelProvider getChannelProvider() {
    return channelProvider;
  }

  Credentials getCredentials() {
    return credentials;
  }

  boolean getUseVirtualThreads() {
    return useVirtualThreads;
  }

  Optional<Duration> getMaxCommitDelay() {
    return maxCommitDelay;
  }

  BuiltInMetricsRecorder getMetricsRecorder() {
    return metricsRecorder;
  }
}

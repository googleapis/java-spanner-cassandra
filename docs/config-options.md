# Config Options

This file documents all the configuration options supported by the Spanner Cassandra Adapter.

```yaml
# [Optional] Global client configurations that apply to all listeners.
globalClientConfigs:
  # [Optional] Enables built-in metrics. Defaults to false. It is highly recommended to enable metrics in production environments for improved debuggability.
  enableBuiltInMetrics: true
  # [Optional] The endpoint for the health check server. If not specified, the health check server will not be started.
  # To check the status, send a GET request to the '/debug/health' path on this endpoint.
  # - A '200 OK' status indicates that the service is healthy.
  # - A '503 Service Unavailable' status indicates that one or more listeners failed to start.
  healthCheckEndpoint: "127.0.0.1:8080"

# A list of all listeners to start.
listeners:
    # The name of the listener. It is recommended to use a meaningful name, such as the cluster name.
  - name: "listener_1"
    # [Optional] The host to bind the listener to. Defaults to "0.0.0.0".
    host: "127.0.0.1"
    # [Optional] The port to bind the listener to. Defaults to 9042.
    port: 9042
    # Spanner configuration for this listener.
    spanner:
      # The URI of the Spanner database.
      databaseUri: "projects/my-project/instances/my-instance/databases/my-database"
      # [Optional] The number of gRPC channels to use. Defaults to 4.
      numGrpcChannels: 4
      # [Optional] The maximum commit delay in milliseconds. Defaults to 0ms.
      # This is the amount of latency this request is willing to incur in order
      # to improve throughput. If this field is not set, Spanner assumes requests
      # are relatively latency sensitive and automatically determines an appropriate
      # delay time.
      maxCommitDelayMillis: 5
```

# Example config.yaml

```yaml
globalClientConfigs:
  enableBuiltInMetrics: true
  healthCheckEndpoint: "127.0.0.1:8080"

listeners:
  - name: "listener_1"
    host: "127.0.0.1"
    port: 9042
    spanner:
      databaseUri: "projects/my-project/instances/my-instance/databases/my-database"
      numGrpcChannels: 4
      maxCommitDelayMillis: 5
  - name: "listener_2"
    host: "127.0.0.2"
    port: 9043
    spanner:
      databaseUri: "projects/my-project/instances/my-instance/databases/my-database-2"
      numGrpcChannels: 8
```

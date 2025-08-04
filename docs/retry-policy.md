# Custom Retry Policy

The Spanner Cassandra driver provides a custom retry policy that is tailored for interacting with Spanner. This policy, `com.google.cloud.spanner.adapter.SpannerCqlRetryPolicy`, extends the default retry behavior of the DataStax Java driver to intelligently handle Spanner-specific transient errors.

## Why Use a Custom Retry Policy?

Spanner may return specific ReadFailure/WriteFailure messages for transient issues that are safe to retry. The default retry policies in the DataStax driver are not aware of these Spanner-specific errors. The `SpannerCqlRetryPolicy` inspects the error messages from Spanner and will trigger a retry for errors that are known to be transient. This improves the resilience of your application and helps it recover from temporary server-side issues without propagating the error to the application layer.

The policy currently retries on the following error messages:
- `HTTP/2 error code: INTERNAL_ERROR`
- `Connection closed with unknown cause`
- `Received unexpected EOS on DATA frame from server`
- `stream terminated by RST_STREAM`
- `Authentication backend internal server error. Please retry.`
- `DEADLINE_EXCEEDED`
- `ABORTED`
- `RESOURCE_EXHAUSTED`
- `UNAVAILABLE`

The policy will retry up to 10 times before re-throwing the exception.

## How to Configure the Retry Policy

You can configure the retry policy in your `application.conf` file. This file is loaded by the DataStax driver when the `CqlSession` is created. You need to specify the custom retry policy class for the execution profiles that you want to use it.

Here is an example of how to set the `SpannerCqlRetryPolicy` as the default retry policy:

```
datastax-java-driver {
  advanced.retry-policy {
    class = com.google.cloud.spanner.adapter.SpannerCqlRetryPolicy
  }
  ...
}
```

You can also configure the policy programmatically in Java:

```java
import com.google.cloud.spanner.adapter.SpannerCqlRetryPolicy;
...
CqlSession session =
        SpannerCqlSession.builder()
            .setDatabaseUri("projects/your_project/instances/your_instance/databases/your_db")
            .addContactEndPoint(yourEndpoint) 
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, SpannerCqlRetryPolicy.class)
                    .build())
            .build();
```
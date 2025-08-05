# View and manage client-side metrics

The Spanner Cassandra client provides client-side metrics that you can use along with server-side metrics to optimize performance and troubleshoot performance issues if they occur.

Client-side metrics are measured from the time a request received in the client to the time client sent response data to the driver. In contrast, server-side metrics are measured from the time Spanner receives a request until the last byte of data is sent to the client.

## Enable client-side metrics

```java
CqlSession session = SpannerCqlSession.builder() 
        .setDatabaseUri(databaseUri)
        .addContactPoint(new InetSocketAddress("localhost", 9042))
        .withLocalDatacenter("datacenter1")
        .setBuiltInMetricsEnabled(true)
        .withConfigLoader(
            DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                .withDuration(
                    DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                .build())
        .build();
```

Client-side metrics are available after you [enable the Cloud Monitoring API](https://console.cloud.google.com/flows/enableapi?apiid=monitoring.googleapis.com).

Client-side metrics are available for users or service accounts that are granted the Identity and Access Management (IAM) monitoring.timeSeries.create permission. This permission is included in the following Spanner IAM roles:

 * [Cloud Spanner Admin](https://cloud.google.com/iam/docs/understanding-roles#spanner.admin) (`roles/spanner.admin`)
 * [Cloud Spanner Database Admin](https://cloud.google.com/iam/docs/understanding-roles#spanner.databaseAdmin) (`roles/spanner.databaseAdmin`)
 * [Cloud Spanner Database Reader](https://cloud.google.com/iam/docs/understanding-roles#spanner.databaseReader) (`roles/spanner.databaseReader`)
 * [Cloud Spanner Database User](https://cloud.google.com/iam/docs/understanding-roles#spanner.databaseUser) (`roles/spanner.databaseUser`)

For more information about granting roles, see [Manage access to projects, folders, and organizations](https://cloud.google.com/iam/docs/granting-changing-revoking-access).

You might also be able to get the required monitoring.timeSeries.create IAM permission through [custom roles](https://cloud.google.com/iam/docs/creating-custom-roles).

## View metrics in the Metrics Explorer

1. In the Google Cloud console, go to the Metrics Explorer page.

     [Go to Metrics Explorer](https://console.cloud.google.com/projectselector/monitoring/metrics-explorer?supportedpurview=project,folder,organizationId)

2. Select your project.

3. Click Select a metric.

4. Search for `spanner.googleapis.com/client`.

5. Select the metric, and then click Apply.

For more information about grouping or aggregating your metric, see [Build queries using menus](https://cloud.google.com/monitoring/charts/metrics-selector#basic-advanced-mode).

Your application needs to run for at least a minute before you can view any published metrics.

## Pricing

There is no charge to view client-side metrics in Cloud Monitoring. 
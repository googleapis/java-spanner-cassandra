# Getting Started with Spanner Cassandra Java Client

<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/googleapis/java-spanner&page=editor&open_in_editor=samples/README.md">
<img alt="Open in Cloud Shell" src ="http://gstatic.com/cloudssh/images/open-btn.png"></a>

[Cloud Spanner][Spanner] is a horizontally-scalable database-as-a-service
with transactions and SQL support.
These sample Java applications demonstrate how to access the Cassandra Spanner API using
the [Spanner Cassandra Client Library for Java][java-spanner-cassandra].

[Spanner]: https://cloud.google.com/spanner/
[java-spanner-cassandra]: https://github.com/googleapis/java-spanner-cassandra

## Quickstart

Install [Maven](http://maven.apache.org/).

Replace the variables `projectId`, `instanceId`, and `databaseId` in the file [`QuickStartSample.java`](snippets/src/main/java/com/example/spanner/cassandra/QuickStartSample.java) with your GCP project, Spanner Instance, and database name respectively.

Build your project from the root directory (`java-spanner-cassandra`):

    mvn clean package -DskipTests
    cd samples/snippets
    mvn package

Every subsequent command here should be run from a subdirectory `samples/snippets`.

### Running samples

Usage:

    java -jar target/spanner-cassandra-snippets/spanner-cassandra-google-cloud-samples.jar

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

package com.google.cloud.spanner.adapter.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.adapter.SpannerCqlRetryPolicy;
import com.google.cloud.spanner.adapter.SpannerCqlSession;
import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder;
import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder.InstanceType;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.common.base.Strings;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.InstanceName;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Manages connection to a Spanner database using Cassandra endpoint */
public class SpannerContext extends DatabaseContext {

  private static final String ENV_VAR_SPANNER_ENDPOINT = "SPANNER_ENDPOINT";
  private static final String INSTANCE_TYPE_PROPERTY = "spanner_cassandra.instance_type";
  private static final String USE_PLAIN_TEXT_PROPERTY = "spanner_cassandra.use_plain_text";
  private static final String CLIENT_CERT_PATH_PROPERTY = "spanner_cassandra.client_cert_path";
  private static final String CLIENT_KEY_PATH_PROPERTY = "spanner_cassandra.client_key_path";
  private static final String SPANNER_OMNI_INSTANCE = "projects/default/instances/default";
  private static final String SPANNER_OMNI_ID = "default";

  private static final String DEFAULT_SPANNER_ENDPOINT = "spanner.googleapis.com:443";

  private final InstanceName instanceName;
  private final String databaseId;
  private final DatabaseName databaseName;

  private DatabaseAdminClient databaseAdminClient;
  private Spanner spannerOmni;
  private CqlSession session;

  private InstanceType getInstanceType() {
    String typeProp = System.getProperty(INSTANCE_TYPE_PROPERTY);
    if (!Strings.isNullOrEmpty(typeProp)) {
      return InstanceType.valueOf(typeProp.toUpperCase());
    }
    if (!Strings.isNullOrEmpty(getSpannerEndpoint())) {
      return InstanceType.OMNI;
    }
    return InstanceType.CLOUD;
  }

  private String getSpannerEndpoint() {
    String endpoint = System.getProperty(ENV_VAR_SPANNER_ENDPOINT);
    if (Strings.isNullOrEmpty(endpoint)) {
      endpoint = System.getenv(ENV_VAR_SPANNER_ENDPOINT);
    }
    return endpoint;
  }

  public SpannerContext() {
    super("Spanner");
    databaseId = keyspace;
    final String instanceNameStr =
        getInstanceType() != InstanceType.OMNI
            ? System.getenv("INTEGRATION_TEST_INSTANCE")
            : SPANNER_OMNI_INSTANCE;
    if (instanceNameStr == null) {
      throw new NullPointerException("Environment variable INTEGRATION_TEST_INSTANCE must be set");
    }
    instanceName = InstanceName.parse(instanceNameStr);
    databaseName =
        DatabaseName.parse(
            String.format(
                "projects/%s/instances/%s/databases/%s",
                instanceName.getProject(), instanceName.getInstance(), databaseId));
  }

  @Override
  public CqlSession getSession() {
    if (session == null) {
      throw new IllegalStateException("initialize() not called.");
    }
    return session;
  }

  @Override
  public void createTables(TableDefinition... tableDefinitions) throws Exception {
    if (databaseAdminClient == null && getInstanceType() != InstanceType.OMNI) {
      throw new IllegalStateException("initialize() not called.");
    }
    List<String> ddls = new ArrayList<>();
    for (TableDefinition tableDefinition : tableDefinitions) {
      ddls.add("DROP TABLE IF EXISTS " + tableDefinition.tableName);
      ddls.add(generateSpannerDdl(tableDefinition.tableName, tableDefinition.columnDefinitions));
    }
    if (getInstanceType() == InstanceType.OMNI) {
      if (spannerOmni == null) {
        throw new IllegalStateException("initialize() not called.");
      }
      spannerOmni
          .getDatabaseAdminClient()
          .updateDatabaseDdl(SPANNER_OMNI_ID, databaseId, ddls, null)
          .get(5, TimeUnit.MINUTES);
    } else {
      databaseAdminClient.updateDatabaseDdlAsync(databaseName, ddls).get(5, TimeUnit.MINUTES);
    }
  }

  private int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) { // 0 finds any available system port
      socket.setReuseAddress(true); // Optional: helps in quickly rebinding if needed
      return socket.getLocalPort();
    }
  }

  private void initializeSpannerOmni() {
    String endpoint = getSpannerEndpoint();
    if (!Strings.isNullOrEmpty(endpoint) && !endpoint.startsWith("http")) {
      if (Boolean.getBoolean(USE_PLAIN_TEXT_PROPERTY)) {
        endpoint = "http://" + endpoint;
      } else {
        endpoint = "https://" + endpoint;
      }
    }

    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setExperimentalHost(endpoint);
    if (Boolean.getBoolean(USE_PLAIN_TEXT_PROPERTY)) {
      builder
          .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
          .setCredentials(NoCredentials.getInstance());
    } else if (!Strings.isNullOrEmpty(System.getProperty(CLIENT_CERT_PATH_PROPERTY))
        && !Strings.isNullOrEmpty(System.getProperty(CLIENT_KEY_PATH_PROPERTY))) {
      builder.useClientCert(
          System.getProperty(CLIENT_CERT_PATH_PROPERTY),
          System.getProperty(CLIENT_KEY_PATH_PROPERTY));
    }
    spannerOmni = builder.build().getService();
  }

  @Override
  public void initialize() throws Exception {
    if (getInstanceType() != InstanceType.OMNI) {
      spannerOmni = null;
      final String env_var_endpoint = System.getenv(ENV_VAR_SPANNER_ENDPOINT);
      DatabaseAdminSettings settings =
          DatabaseAdminSettings.newBuilder()
              .setEndpoint(env_var_endpoint != null ? env_var_endpoint : DEFAULT_SPANNER_ENDPOINT)
              .build();
      databaseAdminClient = DatabaseAdminClient.create(settings);
      databaseAdminClient
          .createDatabaseAsync(instanceName, "CREATE DATABASE " + databaseId)
          .get(5, TimeUnit.MINUTES);
    } else {
      initializeSpannerOmni();
      databaseAdminClient = null;
      spannerOmni
          .getDatabaseAdminClient()
          .createDatabase(
              SPANNER_OMNI_ID,
              "CREATE DATABASE " + databaseId,
              Dialect.GOOGLE_STANDARD_SQL,
              Collections.emptyList())
          .get(5, TimeUnit.MINUTES);
    }

    EndPoint adapterEndpoint =
        new DefaultEndPoint(
            new InetSocketAddress(InetAddress.getLoopbackAddress(), findFreePort()));

    SpannerCqlSessionBuilder sessionBuilder =
        SpannerCqlSession.builder()
            .setDatabaseUri(databaseName.toString())
            .addContactEndPoint(adapterEndpoint) // Configures the internal adapter's port
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withString(
                        DefaultDriverOption.RETRY_POLICY_CLASS,
                        SpannerCqlRetryPolicy.class.getName())
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMinutes(5))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMinutes(5))
                    .withDuration(
                        DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofMinutes(5))
                    .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMinutes(1))
                    .build());
    if (getInstanceType() == InstanceType.OMNI) {
      String endpoint = getSpannerEndpoint();
      if (!Strings.isNullOrEmpty(endpoint)) {
        sessionBuilder.setHost(endpoint);
      }
      sessionBuilder.setInstanceType(InstanceType.OMNI).setUsePlainText(false);
      if (Boolean.getBoolean(USE_PLAIN_TEXT_PROPERTY)) {
        sessionBuilder.setUsePlainText(true);
      } else if (!Strings.isNullOrEmpty(System.getProperty(CLIENT_CERT_PATH_PROPERTY))
          && !Strings.isNullOrEmpty(System.getProperty(CLIENT_KEY_PATH_PROPERTY))) {
        sessionBuilder.setClientCertPathAndKey(
            System.getProperty(CLIENT_CERT_PATH_PROPERTY),
            System.getProperty(CLIENT_KEY_PATH_PROPERTY));
      }
    }
    session = sessionBuilder.build();
  }

  @Override
  public void cleanup() throws Exception {
    if (databaseAdminClient != null) {
      databaseAdminClient.dropDatabase(databaseName);
      databaseAdminClient.close();
    } else if (spannerOmni != null) {
      spannerOmni.getDatabaseAdminClient().dropDatabase(SPANNER_OMNI_ID, databaseId);
      spannerOmni.close();
    }
    if (session != null) {
      session.close();
    }
  }

  private static String generateSpannerDdl(
      String tableName, Map<String, ColumnDefinition> columnDefs) {
    StringBuilder ddl = new StringBuilder(String.format("CREATE TABLE %s (\n  ", tableName));
    List<String> pks = new ArrayList<>();
    List<String> columns = new ArrayList<>();
    columnDefs.forEach(
        (colName, colDef) -> {
          columns.add(
              String.format(
                  "%s %s OPTIONS (cassandra_type = '%s')",
                  colName, colDef.spannerType, colDef.cassandraType));
          if (colDef.primaryKey) {
            pks.add(colName);
          }
        });
    ddl.append(String.join(",\n  ", columns));
    ddl.append(") PRIMARY KEY (");
    ddl.append(String.join(", ", pks));
    ddl.append(")");

    return ddl.toString();
  }
}

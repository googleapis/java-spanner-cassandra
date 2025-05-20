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
import com.google.cloud.spanner.adapter.SpannerCqlSession;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.InstanceName;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Manages connection to a Spanner database using Cassandra endpoint */
public class SpannerContext extends DatabaseContext {

  private static final InstanceName instanceName =
      InstanceName.parse(System.getenv("INTEGRATION_TEST_INSTANCE"));

  private final String databaseId;
  private final DatabaseName databaseName;

  private DatabaseAdminClient databaseAdminClient;
  private CqlSession session;

  public SpannerContext() {
    super("Spanner");
    databaseId = "java_it_test_" + LocalDateTime.now().format(formatter);
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
    if (databaseAdminClient == null) {
      throw new IllegalStateException("initialize() not called.");
    }
    List<String> ddls = new ArrayList<>();
    for (TableDefinition tableDefinition : tableDefinitions) {
      ddls.add("DROP TABLE IF EXISTS " + tableDefinition.tableName);
      ddls.add(generateSpannerDdl(tableDefinition.tableName, tableDefinition.columnDefinitions));
    }
    databaseAdminClient.updateDatabaseDdlAsync(databaseName, ddls).get(5, TimeUnit.MINUTES);
  }

  @Override
  public void initialize() throws Exception {
    databaseAdminClient = DatabaseAdminClient.create();

    databaseAdminClient
        .createDatabaseAsync(instanceName, "CREATE DATABASE " + databaseId)
        .get(5, TimeUnit.MINUTES);

    session =
        SpannerCqlSession.builder()
            .setDatabaseUri(databaseName.toString())
            .withKeyspace(databaseId)
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(60))
                    .build())
            .build();
  }

  @Override
  public void cleanup() throws Exception {
    if (databaseAdminClient != null) {
      databaseAdminClient.dropDatabase(databaseName);
      databaseAdminClient.close();
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

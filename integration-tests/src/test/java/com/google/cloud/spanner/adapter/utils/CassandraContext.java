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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Manages context for interacting with a Cassandra database for integration tests, using an
 * isolated Testcontainers Cassandra instance for each context. Each instance of this context will
 * start its own Cassandra container, manage its own CqlSession, and create/destroy a specific
 * keyspace.
 */
public class CassandraContext extends DatabaseContext {
  private static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:5.0.4");
  private static final int PORT = 9042;
  private CqlSession session;
  private static final CassandraContainer cassandraContainer =
      new CassandraContainer(CASSANDRA_IMAGE)
          .withExposedPorts(PORT)
          .waitingFor(Wait.forLogMessage(".*Starting listening for CQL clients.*\\n", 1))
          .withStartupTimeout(Duration.ofMinutes(3));

  static {
    // Start only one container for all test classes. It will be closed automatically when JVM
    // exits.
    cassandraContainer.start();
  }

  public CassandraContext() {
    super("Cassandra");
  }

  @Override
  public void initialize() throws Exception {
    session =
        CqlSession.builder()
            .addContactPoint(cassandraContainer.getContactPoint())
            .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false)
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMinutes(5))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMinutes(5))
                    .build())
            .build();

    String createKeyspaceCql =
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy',"
                + " 'replication_factor' : 1 };",
            keyspace());
    session.execute(createKeyspaceCql);
    session.execute("USE " + keyspace());
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
    CqlSession session = getSession();
    for (TableDefinition tableDefinition : tableDefinitions) {
      session.execute("DROP TABLE IF EXISTS " + tableDefinition.tableName);
      session.execute(
          generateCassandraDdl(tableDefinition.tableName, tableDefinition.columnDefinitions));
    }
  }

  @Override
  public void cleanup() throws Exception {
    if (session != null && !session.isClosed()) {
      session.close();
    }
  }

  private static String generateCassandraDdl(
      String tableName, Map<String, ColumnDefinition> columnDefs) {
    StringBuilder ddl = new StringBuilder(String.format("CREATE TABLE %s (\n  ", tableName));
    List<String> pks = new ArrayList<>();
    List<String> columns = new ArrayList<>();
    columnDefs.forEach(
        (colName, colDef) -> {
          columns.add(String.format("%s %s", colName, colDef.cassandraType));
          if (colDef.primaryKey) {
            pks.add(colName);
          }
        });
    ddl.append(String.join(",\n  ", columns));
    ddl.append(",\n  PRIMARY KEY (");
    ddl.append(String.join(", ", pks));
    ddl.append(")\n)");

    return ddl.toString();
  }
}

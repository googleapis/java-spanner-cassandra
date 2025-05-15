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
import java.time.LocalDateTime;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Manages context for interacting with a Cassandra database for integration tests, using an
 * isolated Testcontainers Cassandra instance for each context. Each instance of this context will
 * start its own Cassandra container, manage its own CqlSession, and create/destroy a specific
 * keyspace.
 */
public class CassandraContext extends DatabaseContext {

  private static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:latest");
  private static final int PORT = 9042;
  private final String keyspace;

  private CassandraContainer cassandraContainer;
  private CqlSession session;

  public CassandraContext() {
    super("Cassandra");
    keyspace = "java_it_test_" + LocalDateTime.now().format(formatter);
  }

  @Override
  public void initialize() throws Exception {
    try {
      cassandraContainer = new CassandraContainer(CASSANDRA_IMAGE).withExposedPorts(PORT);

      cassandraContainer.start();

      session =
          CqlSession.builder()
              .addContactPoint(cassandraContainer.getContactPoint())
              .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
              .build();

      String createKeyspaceCql =
          String.format(
              "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy',"
                  + " 'replication_factor' : 1 };",
              keyspace);
      session.execute(createKeyspaceCql);
      session.execute("USE " + keyspace);

    } catch (Exception e) {
      if (cassandraContainer != null && cassandraContainer.isRunning()) {
        cassandraContainer.stop();
      }
      throw e;
    }
  }

  @Override
  public CqlSession getSession() {
    if (session == null) {
      throw new IllegalStateException("initialize() not called.");
    }
    return session;
  }

  @Override
  public void executeDdl(String ddl) throws Exception {
    getSession().execute(ddl);
  }

  @Override
  public void cleanup() throws Exception {
    try {
      if (session != null && !session.isClosed()) {
        session.close();
      }
    } finally {
      if (cassandraContainer != null) {
        if (cassandraContainer.isRunning()) {
          cassandraContainer.stop();
        }
        cassandraContainer.close();
      }
    }
  }
}

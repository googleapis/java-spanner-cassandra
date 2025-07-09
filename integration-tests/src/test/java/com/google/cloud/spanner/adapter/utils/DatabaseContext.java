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
import java.time.format.DateTimeFormatter;

/**
 * Abstract base class for defining a database context. Each concrete context (Spanner, Cassandra)
 * will provide its specific implementation and database cleanup mechanism.
 */
public abstract class DatabaseContext {

  protected static final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
  private final String name;
  protected final String keyspace;

  public DatabaseContext(String name) {
    this.name = name;
    keyspace = "java_it_test_" + LocalDateTime.now().format(formatter);
  }

  @Override
  public String toString() {
    return name;
  }

  public String keyspace() {
    return keyspace;
  }

  /** Initializes the database */
  public abstract void initialize() throws Exception;

  /** Deletes the underlying database */
  public abstract void cleanup() throws Exception;

  /** Provides the default initialized {@code CqlSession} for this database context. */
  public abstract CqlSession getSession();

  /**
   * Creates the specified tables in the database. Each database is responsible for generating its
   * own DDL from the provided column definitions.
   */
  public abstract void createTables(TableDefinition... tableDefinitions) throws Exception;
}

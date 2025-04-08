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

package com.example.spanner.cassandra;

// [START spanner_cassandra_quick_start]

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.SpannerCqlSession;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Random;

// This sample assumes your spanner database <my_db> contains a table <users>
// with the following schema:
/*
CREATE TABLE users (
	id   INT64          OPTIONS (cassandra_type = 'int'),
	active    BOOL           OPTIONS (cassandra_type = 'boolean'),
	username  STRING(MAX)    OPTIONS (cassandra_type = 'text'),
) PRIMARY KEY (id);
*/
class QuickStartSample {

  public static void main(String[] args) {

    // TODO(developer): Replace these variables before running the sample.
    final String projectId = "my-gcp-project";
    final String instanceId = "my-spanner-instance";
    final String databaseId = "my_db";

    final String databaseUri =
        String.format("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId);

    try (CqlSession session =
        SpannerCqlSession.builder() // `SpannerCqlSession` instead of `CqlSession`
            .setDatabaseUri(databaseUri) // Set spanner database URI.
            .addContactPoint(new InetSocketAddress("localhost", 9042))
            .withLocalDatacenter("datacenter1")
            .withKeyspace(databaseId) // Keyspace name should be the same as spanner database name
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                    .build())
            .build()) {

      final int randomUserId = new Random().nextInt(Integer.MAX_VALUE);

      System.out.printf("Inserting user with ID: %d%n", randomUserId);

      // INSERT data
      session.execute(
          "INSERT INTO users (id, active, username) VALUES (?, ?, ?)",
          randomUserId,
          true,
          "John Doe");

      System.out.printf("Successfully inserted user: %d%n", randomUserId);
      System.out.printf("Querying user: %d%n", randomUserId);

      // SELECT data
      ResultSet rs =
          session.execute("SELECT id, active, username FROM users WHERE id = ?", randomUserId);

      // Get the first row from the result set
      Row row = rs.one();

      System.out.printf(
          "%d %b %s%n", row.getInt("id"), row.getBoolean("active"), row.getString("username"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

// [END spanner_cassandra_quick_start]

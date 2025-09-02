/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spanner.cassandra;

// [START spanner_cassandra_quick_start]

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.SpannerCqlSession;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

// This sample assumes your spanner database <my_db> contains a table <users>
// with the following schema:

// CREATE TABLE users (
//  id        INT64          OPTIONS (cassandra_type = 'int'),
//  active    BOOL           OPTIONS (cassandra_type = 'boolean'),
//  username  STRING(MAX)    OPTIONS (cassandra_type = 'text'),
// ) PRIMARY KEY (id);

class QuickStartSample {

  public static void main(String[] args) {

    // TODO(developer): Replace these variables before running the sample.
    final String projectId = "span-cloud-testing";
    final String instanceId = "c2sp";
    final String databaseId = "yahoo";

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

      PreparedStatement prepared =
          session.prepare("INSERT INTO users (id, active, username) VALUES (?, ?, ?)");
      PreparedStatement prepared1 =
          session.prepare("SELECT id, active, username FROM users WHERE id = ?");

      PreparedStatement insert =
          session.prepare(
              "INSERT INTO native_address_book_entries_v2 (guid, source, device_id,"
                  + " entry_hash, local_ids) VALUES (?, ?, ?, ?, ?);");

      PreparedStatement update =
          session.prepare(
              "UPDATE native_address_book_entries_v2 SET local_ids[?] = false WHERE guid = ? AND"
                  + " source = ? AND device_id = ? AND entry_hash = ?;");

      PreparedStatement select =
          session.prepare(
              "SELECT local_ids, source FROM native_address_book_entries_v2 WHERE guid = ?");

      Map<String, Boolean> userAttributes = new HashMap<>();
      userAttributes.put("theme", true);
      userAttributes.put("language", true);

      // BoundStatement bound = insert.bind("John Doe", "source", "device_id", 10, userAttributes);

      BoundStatement boundInsert =
          insert
              .bind()
              .setString("guid", "mk")
              .setString("source", "source")
              .setString("device_id", "device_id")
              .setInt("entry_hash", 2211)
              .setMap("local_ids", userAttributes, String.class, Boolean.class);

      // 3. EXECUTE the bound statement
      session.execute(boundInsert);

      BoundStatement boundUpdate = update.bind("language", "mk", "source", "device_id", 2211);

      // 3. EXECUTE the bound statement
      session.execute(boundUpdate);

      BoundStatement boundSelect = select.bind("mk");

      // SELECT data
      ResultSet rs = session.execute(boundSelect);

      // Get the first row from the result set
      Row row = rs.one();

      Map<String, Boolean> retrievedFlags = row.getMap("local_ids", String.class, Boolean.class);

      retrievedFlags.forEach(
          (feature, isEnabled) -> System.out.println("   - " + feature + ": " + isEnabled));

      System.out.printf("Source: %s%n", row.getString("source"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

// [END spanner_cassandra_quick_start]

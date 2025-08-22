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
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.cloud.spanner.adapter.SpannerCqlSession;
import java.net.InetSocketAddress;
import java.time.Duration;

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
    final String databaseId = "xobni-derived";

    final String databaseUri =
        String.format("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId);

    try (CqlSession session =
        SpannerCqlSession.builder() // `SpannerCqlSession` instead of `CqlSession`
            .setDatabaseUri(databaseUri) // Set spanner database URI.
            .addContactPoint(new InetSocketAddress("localhost", 9042))
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                    .build())
            .build()) {

      PreparedStatement stmt1 =
          session.prepare(
              "SELECT mbr_guid FROM xobni_derived.user_config_by_bucket WHERE bucket = ?");
      PreparedStatement stmt2 =
          session.prepare(
              "SELECT mbr_guid, migration_status FROM xobni_derived.user_config_by_bucket WHERE"
                  + " bucket = ?;");
      PreparedStatement stmt3 =
          session.prepare(
              "SELECT user_configuration FROM xobni_derived.user_config_by_bucket WHERE bucket = ?"
                  + " AND mbr_guid = ?;");
      PreparedStatement stmt4 =
          session.prepare(
              "SELECT user_configuration, onboard_status, migration_status FROM"
                  + " xobni_derived.user_config_by_bucket WHERE bucket = ? AND mbr_guid = ?;");
      PreparedStatement stmt5 =
          session.prepare(
              "SELECT mbr_guid, user_configuration, onboard_status, migration_status,"
                  + " migration_note FROM xobni_derived.user_config_by_bucket WHERE bucket = ?;");
      PreparedStatement stmt6 =
          session.prepare(
              "INSERT INTO xobni_derived.user_config_by_bucket (bucket, mbr_guid,"
                  + " user_configuration) VALUES (?, ?, ?);");
      PreparedStatement stmt7 =
          session.prepare(
              "INSERT INTO xobni_derived.user_config_by_bucket (bucket, mbr_guid,"
                  + " user_configuration, onboard_status) VALUES (?, ?, ?, ?);");
      PreparedStatement stmt8 =
          session.prepare(
              "INSERT INTO xobni_derived.user_config_by_bucket (bucket, mbr_guid, onboard_status)"
                  + " VALUES (?, ?, ?);");

      PreparedStatement stmt9 =
          session.prepare(
              "DELETE FROM xobni_derived.user_config_by_bucket WHERE bucket = ? AND mbr_guid = ?;");
      PreparedStatement stmt81 =
          session.prepare(
              "INSERT INTO xobni_derived.user_config_by_bucket (bucket, mbr_guid, migration_status)"
                  + " VALUES (?, ?, ?);");
      PreparedStatement stmt71 =
          session.prepare(
              "INSERT INTO xobni_derived.user_config_by_bucket (bucket, mbr_guid, migration_status,"
                  + " migration_note) VALUES (?, ?, ?, ?);");
      PreparedStatement stmt811 =
          session.prepare(
              "SELECT migration_status FROM xobni_derived.user_config_by_bucket WHERE bucket = ?"
                  + " AND mbr_guid = ?;");
      PreparedStatement stmt711 =
          session.prepare(
              "SELECT migration_status, migration_note FROM xobni_derived.user_config_by_bucket"
                  + " WHERE bucket = ? AND mbr_guid = ?;");
      PreparedStatement stmt8111 =
          session.prepare(
              "SELECT mbr_guid, contact_id, method, prose, message_count, first_endpoint,"
                  + " first_message_id, first_observation, last_message_id, last_observation,"
                  + " histogram FROM xobni_derived.relationship_history_cache WHERE mbr_guid = ?;");
      PreparedStatement stmt522 =
          session.prepare(
              "SELECT mbr_guid, contact_id, method, prose, message_count, first_endpoint,"
                  + " first_observation, last_observation, histogram, first_message_id,"
                  + " last_message_id FROM xobni_derived.relationship_history_cache WHERE mbr_guid"
                  + " = ? AND contact_id = ?;");
      PreparedStatement stmt622 =
          session.prepare(
              "INSERT INTO xobni_derived.relationship_history_cache (mbr_guid, contact_id, method,"
                  + " prose, message_count, first_endpoint, first_message_id, first_observation,"
                  + " last_message_id, last_observation, histogram) VALUES (?, ?, ?, ?, ?, ?, ?, ?,"
                  + " ?, ?, ?);");
      PreparedStatement stmt722 =
          session.prepare(
              "DELETE FROM xobni_derived.relationship_history_cache WHERE mbr_guid = ? AND"
                  + " contact_id = ?;");
      PreparedStatement stmt822 =
          session.prepare(
              "DELETE FROM xobni_derived.relationship_history_cache WHERE mbr_guid = ?;");
      PreparedStatement stmt8221 =
          session.prepare(
              "SELECT * FROM xobni_derived.user_ep_table_int_to_id WHERE mbr_guid = ? AND ep_int"
                  + " IN ?;");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

// [END spanner_cassandra_quick_start]

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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.SpannerCqlSession;
import java.net.InetSocketAddress;
import java.time.Duration;

/*
 * This sample demonstrating how you might create multiple CqlSession instances,
 * perhaps for different Spanner databases. You can simply build them independently,
 * configuring each one as needed.
 *
 * This is mainly for the application which needs to interact with entirely separate
 * Spanner databases.
 */
public class MultipleCqlSessions {

  public static void main(String[] args) {

    CqlSession session1 = null;
    CqlSession session2 = null;
    CqlSession session3 = null;
    final String databaseUri1 = "<replace_with_your_database_uri_here>";
    final String databaseUri2 = "<replace_with_your_database_uri_here>";
    final String databaseUri3 = "<replace_with_your_database_uri_here>";

    try {
      System.out.println("Building Session 1...");
      session1 =
          SpannerCqlSession.builder()
              .setDatabaseUri(databaseUri1)
              .addContactPoint(new InetSocketAddress("localhost", 9042))
              .withLocalDatacenter("datacenter1")
              .withConfigLoader(
                  DriverConfigLoader.programmaticBuilder()
                      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                      .withDuration(
                          DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                      .build())
              .build();
      System.out.println("Session 1 connected to cluster: " + session1.getName());

      // Execute a query using session1
      ResultSet rs1 = session1.execute("SELECT user_id from users");
      Row row1 = rs1.one();
      if (row1 != null) {
        System.out.println("Session 1 - user_id: " + row1.getString("user_id"));
      }

      System.out.println("\nBuilding Session 2...");
      session2 =
          SpannerCqlSession.builder()
              .setDatabaseUri(databaseUri2)
              .addContactPoint(new InetSocketAddress("localhost", 9043))
              .withLocalDatacenter("datacenter1")
              .withConfigLoader(
                  DriverConfigLoader.programmaticBuilder()
                      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                      .withDuration(
                          DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                      .build())
              .build();
      System.out.println("Session 2 connected to cluster: " + session2.getName());

      // Execute a query using session2
      ResultSet rs2 = session2.execute("SELECT user_id from users");
      Row row2 = rs2.one();
      if (row2 != null) {
        System.out.println("Session 2 - user_id: " + row2.getUuid("user_id"));
      }

      System.out.println("\nBuilding Session 3...");
      session3 =
          SpannerCqlSession.builder()
              .setDatabaseUri(databaseUri3)
              .addContactPoint(new InetSocketAddress("localhost", 9044))
              .withLocalDatacenter("datacenter1")
              .withConfigLoader(
                  DriverConfigLoader.programmaticBuilder()
                      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                      .withDuration(
                          DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                      .build())
              .build();
      System.out.println("Session 3 connected to cluster: " + session3.getName());

      // Execute a query using session3
      ResultSet rs3 = session3.execute("SELECT user_id from users");
      Row row3 = rs3.one();
      if (row3 != null) {
        System.out.println("Session 3 - user_id: " + row3.getUuid("user_id"));
      }

    } catch (Exception e) {
      System.err.println("An error occurred: " + e.getMessage());
      e.printStackTrace();
    } finally {
      // Always close sessions when done to free resources
      if (session1 != null) {
        session1.close();
        System.out.println("Session 1 closed.");
      }
      if (session2 != null) {
        session2.close();
        System.out.println("Session 2 closed.");
      }
      if (session3 != null) {
        session3.close();
        System.out.println("Session 3 closed.");
      }
    }
  }
}

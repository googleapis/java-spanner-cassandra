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
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;

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
        CqlSession.builder() // `SpannerCqlSession` instead of `CqlSession`
            .addContactPoint(new InetSocketAddress("localhost", 9044))
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                    .build())
            .build()) {

      PreparedStatement updateStmt =
          session.prepare("UPDATE yahoo.users SET active = ?, username = ? WHERE id = ?");
      BoundStatement updateBound = updateStmt.bind(true, "source", 123456789);
      updateBound = updateBound.unset(1);

      // 3. EXECUTE the bound statement
      session.execute(updateBound);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

// [END spanner_cassandra_quick_start]

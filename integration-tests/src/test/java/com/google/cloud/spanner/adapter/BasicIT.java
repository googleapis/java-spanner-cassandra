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

package com.google.cloud.spanner.adapter;

import static com.google.common.truth.Truth.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Test;

/** Basic integration test that creates a table, inserts some random data, reads and verifies it */
public class BasicIT extends AbstractIT {

  public BasicIT(DatabaseContext db) {
    super(db);
  }

  @Test
  public void basicTest() throws Exception {
    // Create table
    Map<String, ColumnDefinition> columnDefs = new HashMap<>();
    columnDefs.put("id", new ColumnDefinition("INT64", "INT", true));
    columnDefs.put("active", new ColumnDefinition("BOOL", "BOOLEAN", false));
    columnDefs.put("username", new ColumnDefinition("STRING(MAX)", "TEXT", false));
    db.createTables(new TableDefinition("users", columnDefs));

    final int randomUserId = new Random().nextInt(Integer.MAX_VALUE);
    CqlSession session = db.getSession();

    // Insert data
    session.execute(
        "INSERT INTO users (id, active, username) VALUES (?, ?, ?)",
        randomUserId,
        true,
        "John Doe");

    // Read back the data
    ResultSet rs =
        session.execute("SELECT id, active, username FROM users WHERE id = ?", randomUserId);
    Row row = rs.one();

    assertThat(row.getInt("id")).isEqualTo(randomUserId);
    assertThat(row.getBoolean("active")).isTrue();
    assertThat(row.getString("username")).isEqualTo("John Doe");
  }
}

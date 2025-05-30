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

package com.google.cloud.spanner.adapter.cql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class RandomSessionQueryGeneratorIT extends AbstractIT {

  private static final String TABLE_NAME = "user_sessions";
  private static CqlSession session;
  private static RandomSessionQueryGenerator queryGenerator;

  public RandomSessionQueryGeneratorIT(DatabaseContext db) {
    super(db);
    session = db.getSession();
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("session_id", new ColumnDefinition("STRING(MAX)", "text", true));
    testColumns.put("user_id", new ColumnDefinition("STRING(MAX)", "text", false));
    testColumns.put("start_time", new ColumnDefinition("TIMESTAMP", "timestamp", false));
    testColumns.put("end_time", new ColumnDefinition("TIMESTAMP", "timestamp", false));
    testColumns.put("duration_minutes", new ColumnDefinition("INT64", "int", false));
    testColumns.put("device_type", new ColumnDefinition("STRING(MAX)", "text", false));
    testColumns.put("is_active", new ColumnDefinition("BOOL", "boolean", false));
    testColumns.put("session_properties", new ColumnDefinition("JSON", "map<text, text>", false));
    testColumns.put("tags", new ColumnDefinition("ARRAY<STRING(MAX)>", "set<text>", false));
    TableDefinition user_sessions = new TableDefinition(TABLE_NAME, testColumns);
    db.createTables(user_sessions);
    queryGenerator = new RandomSessionQueryGenerator(session, db.keyspace(), TABLE_NAME);
  }

  @Test
  public void testGeneratedInsertAndSelectQuery() {
    // Generate a random INSERT statement with its corresponding data
    RandomSessionQueryGenerator.SessionDataAndStatement insertDataAndStmt =
        queryGenerator.generateRandomInsert();
    RandomSessionQueryGenerator.SessionData expectedData = insertDataAndStmt.sessionData;

    session.execute(insertDataAndStmt.statement);

    // Generate a SELECT statement for the inserted data
    BoundStatement selectStmt = queryGenerator.generateSelectBySessionId(expectedData.sessionId);
    ResultSet rs = session.execute(selectStmt);
    Row row = rs.one();

    assertNotNull(row);

    // Map the retrieved row back to our data object
    RandomSessionQueryGenerator.SessionData actualData =
        RandomSessionQueryGenerator.SessionData.fromRow(row);

    // Assert that the retrieved data matches the generated data
    assertEquals(expectedData, actualData);
  }

  @Test
  public void testMultipleRandomInserts() {
    int numInserts = 10; // Test inserting multiple random entries
    for (int i = 0; i < numInserts; i++) {
      RandomSessionQueryGenerator.SessionDataAndStatement insertDataAndStmt =
          queryGenerator.generateRandomInsert();
      session.execute(insertDataAndStmt.statement);

      // Immediately verify the inserted data
      BoundStatement selectStmt =
          queryGenerator.generateSelectBySessionId(insertDataAndStmt.sessionData.sessionId);
      Row row = session.execute(selectStmt).one();
      assertNotNull(row);
      RandomSessionQueryGenerator.SessionData actualData =
          RandomSessionQueryGenerator.SessionData.fromRow(row);
      assertEquals(insertDataAndStmt.sessionData, actualData);
    }

    // Verify total count (requires ALLOW FILTERING or a specific query pattern)
    // For simple count, this often needs ALLOW FILTERING if not using a specific partition key
    // Not ideal for large-scale production, but okay for integration test verification.
    ResultSet countRs = session.execute("SELECT COUNT(*) FROM " + TABLE_NAME + " ALLOW FILTERING");
    assertEquals(numInserts, countRs.one().getLong(0));
  }
}

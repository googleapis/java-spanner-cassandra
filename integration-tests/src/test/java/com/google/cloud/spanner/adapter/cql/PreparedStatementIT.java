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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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

public class PreparedStatementIT extends AbstractIT {

  public PreparedStatementIT(DatabaseContext db) {
    super(db);
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("a", new ColumnDefinition("INT64", "INT", true));
    testColumns.put("b", new ColumnDefinition("INT64", "INT", false));
    testColumns.put("c", new ColumnDefinition("INT64", "INT", false));
    TableDefinition prepared_statement_test =
        new TableDefinition("prepared_statement_test", testColumns);
    db.createTables(prepared_statement_test);

    for (int i = 1; i <= 4; i++) {
      db.getSession()
          .execute(
              SimpleStatement.builder(
                      "INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?)")
                  .addPositionalValues(i, i, i)
                  .build());
    }
  }

  @Test
  public void should_have_empty_result_definitions_for_insert_query_without_bound_variable() {
    CqlSession session = db.getSession();
    PreparedStatement prepared =
        session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)");
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  public void should_have_non_empty_result_definitions_for_insert_query_with_bound_variable() {
    CqlSession session = db.getSession();
    PreparedStatement prepared =
        session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?)");
    assertThat(prepared.getVariableDefinitions()).hasSize(3);
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  public void should_have_empty_variable_definitions_for_select_query_without_bound_variable() {
    CqlSession session = db.getSession();
    PreparedStatement prepared =
        session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = 1");
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertThat(prepared.getResultSetDefinitions()).hasSize(3);
  }

  @Test
  public void should_have_non_empty_variable_definitions_for_select_query_with_bound_variable() {
    CqlSession session = db.getSession();
    PreparedStatement prepared =
        session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = ?");
    assertThat(prepared.getVariableDefinitions()).hasSize(1);
    assertThat(prepared.getResultSetDefinitions()).hasSize(3);
  }
}

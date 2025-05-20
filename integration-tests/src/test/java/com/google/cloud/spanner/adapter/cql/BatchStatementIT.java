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

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class BatchStatementIT extends AbstractIT {
  private static final int batchCount = 100;

  public BatchStatementIT(DatabaseContext db) {
    super(db);
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("k0", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    testColumns.put("k1", new ColumnDefinition("INT64", "INT", true));
    testColumns.put("v", new ColumnDefinition("INT64", "INT", false));
    TableDefinition test = new TableDefinition("test", testColumns);

    Map<String, ColumnDefinition> counter1Columns = new HashMap<>();
    counter1Columns.put("k0", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    counter1Columns.put("c", new ColumnDefinition("INT64", "COUNTER", false));
    TableDefinition counter1 = new TableDefinition("counter1", counter1Columns);

    Map<String, ColumnDefinition> counter2Columns = new HashMap<>();
    counter2Columns.put("k0", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    counter2Columns.put("c", new ColumnDefinition("INT64", "COUNTER", false));
    TableDefinition counter2 = new TableDefinition("counter2", counter2Columns);

    Map<String, ColumnDefinition> counter3Columns = new HashMap<>();
    counter3Columns.put("k0", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    counter3Columns.put("c", new ColumnDefinition("INT64", "COUNTER", false));
    TableDefinition counter3 = new TableDefinition("counter3", counter3Columns);

    db.createTables(test, counter1, counter2, counter3);
  }

  @Test
  public void should_execute_batch_of_simple_statements_with_variables() {
    // Build a batch of batchCount simple statements, each with their own positional variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getMethodName()))
              .addPositionalValues(i, i + 1)
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    db.getSession().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  public void should_execute_batch_of_bound_statements_with_variables() {
    // Build a batch of batchCount statements with bound statements, each with their own positional
    // variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = db.getSession().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    db.getSession().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  public void should_execute_batch_of_bound_statements_with_unset_values() {
    // Build a batch of batchCount statements with bound statements, each with their own positional
    // variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = db.getSession().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    db.getSession().execute(batchStatement);

    verifyBatchInsert();

    BatchStatementBuilder builder2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      BoundStatement boundStatement = preparedStatement.bind(i, i + 2);
      // unset v every 20 statements.
      if (i % 20 == 0) {
        boundStatement = boundStatement.unset(1);
      }
      builder.addStatement(boundStatement);
    }

    db.getSession().execute(builder2.build());

    Statement<?> select =
        SimpleStatement.builder("SELECT * from test where k0 = ?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = db.getSession().execute(select);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(100);

    Iterator<Row> iterator = rows.iterator();
    for (int i = 0; i < batchCount; i++) {
      Row row = iterator.next();
      assertThat(row.getString("k0")).isEqualTo(name.getMethodName());
      assertThat(row.getInt("k1")).isEqualTo(i);
      // value should be from first insert (i + 1) if at row divisble by 20, otherwise second.
      int expectedValue = i % 20 == 0 ? i + 1 : i + 2;
      if (i % 20 == 0) {
        assertThat(row.getInt("v")).isEqualTo(expectedValue);
      }
    }
  }

  @Test
  public void should_execute_batch_of_bound_and_simple_statements_with_variables() {
    // Build a batch of batchCount statements with simple and bound statements alternating.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = db.getSession().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      if (i % 2 == 1) {
        SimpleStatement simpleInsert =
            SimpleStatement.builder(
                    String.format(
                        "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getMethodName()))
                .addPositionalValues(i, i + 1)
                .build();
        builder.addStatement(simpleInsert);
      } else {
        builder.addStatement(preparedStatement.bind(i, i + 1));
      }
    }

    BatchStatement batchStatement = builder.build();
    db.getSession().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  public void should_execute_counter_batch() {
    // should be able to do counter increments in a counter batch.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.COUNTER);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getMethodName()))
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    db.getSession().execute(batchStatement);

    for (int i = 1; i <= 3; i++) {
      ResultSet result =
          db.getSession()
              .execute(
                  String.format(
                      "SELECT c from counter%d where k0 = '%s'", i, name.getMethodName()));

      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);

      Row row = rows.iterator().next();
      assertThat(row.getLong("c")).isEqualTo(i);
    }
  }

  private void verifyBatchInsert() {
    // validate data inserted by the batch.
    Statement<?> select =
        SimpleStatement.builder("SELECT * from test where k0 = ?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = db.getSession().execute(select);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(100);

    Iterator<Row> iterator = rows.iterator();
    for (int i = 0; i < batchCount; i++) {
      Row row = iterator.next();
      assertThat(row.getString("k0")).isEqualTo(name.getMethodName());
      assertThat(row.getInt("k1")).isEqualTo(i);
      assertThat(row.getInt("v")).isEqualTo(i + 1);
    }
  }
}

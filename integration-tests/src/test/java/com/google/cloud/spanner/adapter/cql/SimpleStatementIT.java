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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class SimpleStatementIT extends AbstractIT {
  private static final String KEY = "test";

  public SimpleStatementIT(DatabaseContext db) {
    super(db);
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    // table where every column forms the primary key.
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("k", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    testColumns.put("v", new ColumnDefinition("INT64", "INT", true));
    TableDefinition test = new TableDefinition("test", testColumns);

    Map<String, ColumnDefinition> testColumns2 = new HashMap<>();
    testColumns2.put("k", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    testColumns2.put("v", new ColumnDefinition("INT64", "INT", false));
    TableDefinition test2 = new TableDefinition("test2", testColumns2);
    db.createTables(test, test2);

    for (int i = 0; i < 100; i++) {
      db.getSession()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }
  }

  @Test
  public void should_use_positional_values() {
    // given a statement with positional values
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test2 (k, v) values (?, ?)")
            .addPositionalValue(name.getMethodName())
            .addPositionalValue(4)
            .build();

    // when executing that statement
    db.getSession().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = db.getSession().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    assertThat(row.getString("k")).isEqualTo(name.getMethodName());
    assertThat(row.getInt("v")).isEqualTo(4);
  }

  @Test
  public void should_allow_nulls_in_positional_values() {
    // given a statement with positional values
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test2 (k, v) values (?, ?)")
            .addPositionalValue(name.getMethodName())
            .addPositionalValue(null)
            .build();

    // when executing that statement
    db.getSession().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = db.getSession().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    assertThat(row.getString("k")).isEqualTo(name.getMethodName());
    assertThat(row.getObject("v")).isNull();
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_when_too_many_positional_values_provided() {
    // given a statement with more bound values than anticipated (3 given vs. 2 expected)
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test (k, v) values (?, ?)")
            .addPositionalValues(KEY, 0, 7)
            .build();

    // when executing that statement
    db.getSession().execute(insert);

    // then the server will throw an InvalidQueryException which is thrown up to the client.
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_when_not_enough_positional_values_provided() {
    // given a statement with not enough bound values (1 given vs. 2 expected)
    SimpleStatement insert =
        SimpleStatement.builder("SELECT * from test where k = ? and v = ?")
            .addPositionalValue(KEY)
            .build();

    // when executing that statement
    db.getSession().execute(insert);

    // then the server will throw an InvalidQueryException which is thrown up to the client.
  }
}

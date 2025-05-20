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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class BoundStatementIT extends AbstractIT {
  private static final String KEY = "test";
  private static final int VALUE = 7;

  public BoundStatementIT(DatabaseContext db) {
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
    // table with simple primary key, single cell.
    Map<String, ColumnDefinition> test2Columns = new HashMap<>();
    test2Columns.put("k", new ColumnDefinition("STRING(MAX)", "TEXT", true));
    test2Columns.put("v0", new ColumnDefinition("INT64", "INT", false));
    TableDefinition test2 = new TableDefinition("test2", test2Columns);
    // table with composite partition key
    Map<String, ColumnDefinition> test3Columns = new HashMap<>();
    test3Columns.put("pk1", new ColumnDefinition("INT64", "INT", true));
    test3Columns.put("pk2", new ColumnDefinition("INT64", "INT", true));
    test3Columns.put("v", new ColumnDefinition("INT64", "INT", false));
    TableDefinition test3 = new TableDefinition("test3", test3Columns);
    db.createTables(test, test2, test3);

    for (int i = 0; i < 100; i++) {
      db.getSession()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }
  }

  @Test
  public void should_have_empty_result_definitions_for_update_query() {
    PreparedStatement prepared = db.getSession().prepare("INSERT INTO test2 (k, v0) values (?, ?)");

    assertThat(prepared.getResultSetDefinitions()).hasSize(0);

    ResultSet rs = db.getSession().execute(prepared.bind(name.getMethodName(), VALUE));
    assertThat(rs.getColumnDefinitions()).hasSize(0);
  }

  @Test
  public void should_bind_null_value_when_setting_values_in_bulk() {
    PreparedStatement prepared = db.getSession().prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    BoundStatement boundStatement = prepared.bind(name.getMethodName(), null);
    assertThat(boundStatement.get(1, TypeCodecs.INT)).isNull();
  }

  @Test
  public void should_propagate_attributes_when_preparing_a_simple_statement() {
    CqlSession session = db.getSession();

    DriverExecutionProfile mockProfile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            // Value doesn't matter, we just want a distinct profile
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    // TODO: Add attribute for paging and custom payload after supported.
    Duration mockTimeout = Duration.ofSeconds(1);
    int mockPageSize = 2000;

    SimpleStatementBuilder simpleStatementBuilder =
        SimpleStatement.builder("SELECT release_version FROM system.local")
            .setExecutionProfile(mockProfile)
            .setQueryTimestamp(42)
            .setIdempotence(true)
            .setTracing()
            .setTimeout(mockTimeout)
            .setPageSize(mockPageSize);

    PreparedStatement preparedStatement = session.prepare(simpleStatementBuilder.build());

    // Cover all the ways to create bound statements:
    ImmutableList<Function<PreparedStatement, BoundStatement>> createMethods =
        ImmutableList.of(PreparedStatement::bind, p -> p.boundStatementBuilder().build());

    for (Function<PreparedStatement, BoundStatement> createMethod : createMethods) {
      BoundStatement boundStatement = createMethod.apply(preparedStatement);

      assertThat(boundStatement.getExecutionProfile()).isEqualTo(mockProfile);
      assertThat(boundStatement.isIdempotent()).isTrue();
      assertThat(boundStatement.isTracing()).isTrue();
      assertThat(boundStatement.getTimeout()).isEqualTo(mockTimeout);
      assertThat(boundStatement.getPageSize()).isEqualTo(mockPageSize);

      // Bound statements do not support per-query keyspaces, so this is not set
      assertThat(boundStatement.getKeyspace()).isNull();
      // Should not be propagated
      assertThat(boundStatement.getQueryTimestamp()).isEqualTo(Statement.NO_DEFAULT_TIMESTAMP);
    }
  }
}

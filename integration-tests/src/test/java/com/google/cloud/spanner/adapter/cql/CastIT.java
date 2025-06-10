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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class CastIT extends AbstractIT {

  private static final String TABLE_NAME = "cast_test_table";
  private static CqlSession session;

  public CastIT(DatabaseContext db) {
    super(db);
    session = db.getSession();
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("id", new ColumnDefinition("STRING(MAX)", "UUID", true));
    testColumns.put("text_col", new ColumnDefinition("STRING(MAX)", "text", false));
    testColumns.put("int_col", new ColumnDefinition("INT64", "INT", false));
    testColumns.put("bigint_col", new ColumnDefinition("INT64", "BIGINT", false));
    testColumns.put("float_col", new ColumnDefinition("FLOAT64", "FLOAT", false));
    testColumns.put("double_col", new ColumnDefinition("FLOAT64", "DOUBLE", false));
    testColumns.put("boolean_col", new ColumnDefinition("BOOL", "BOOLEAN", false));
    testColumns.put("timestamp_col", new ColumnDefinition("TIMESTAMP", "TIMESTAMP", false));
    testColumns.put("date_col", new ColumnDefinition("DATE", "DATE", false));
    TableDefinition cast_table = new TableDefinition(TABLE_NAME, testColumns);
    db.createTables(cast_table);
  }

  @Test
  public void testCastIntToText() {
    UUID id = UUID.randomUUID();
    session.execute(
        QueryBuilder.insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(id))
            .value("int_col", QueryBuilder.literal(456))
            .build());

    ResultSet rs =
        session.execute(
            "SELECT CAST(int_col AS text) AS casted_value FROM "
                + TABLE_NAME
                + " WHERE id = "
                + id);
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getString("casted_value")).isEqualTo("456");
  }

  @Test
  public void testCastFloatToDouble() {
    UUID id = UUID.randomUUID();
    session.execute(
        QueryBuilder.insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(id))
            .value("float_col", QueryBuilder.literal(123.45f))
            .build());

    ResultSet rs =
        session.execute(
            "SELECT CAST(float_col AS double) AS casted_value FROM "
                + TABLE_NAME
                + " WHERE id = "
                + id);
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getDouble("casted_value"))
        .isEqualTo(123.45, org.assertj.core.api.Assertions.within(0.0001));
  }

  @Test
  public void testCastDoubleToText() {
    UUID id = UUID.randomUUID();
    session.execute(
        QueryBuilder.insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(id))
            .value("double_col", QueryBuilder.literal(987.65d))
            .build());

    ResultSet rs =
        session.execute(
            "SELECT CAST(double_col AS text) AS casted_value FROM "
                + TABLE_NAME
                + " WHERE id = "
                + id);
    Row row = rs.one();
    assertThat(row).isNotNull();
    // Note: Floating point to string conversion can be tricky, check for expected format.
    assertThat(row.getString("casted_value")).isEqualTo("987.65");
  }

  @Test
  public void testCastTimestampToText() {
    UUID id = UUID.randomUUID();
    Instant now = Instant.parse("2025-06-10T10:30:00Z");
    session.execute(
        QueryBuilder.insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(id))
            .value("timestamp_col", QueryBuilder.literal(now))
            .build());

    ResultSet rs =
        session.execute(
            "SELECT CAST(timestamp_col AS text) AS casted_value FROM "
                + TABLE_NAME
                + " WHERE id = "
                + id);
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getString("casted_value")).startsWith("2025-06-10");
  }

  @Test
  public void testCastDateToText() {
    UUID id = UUID.randomUUID();
    LocalDate today = LocalDate.of(2025, 6, 10);
    session.execute(
        QueryBuilder.insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(id))
            .value("date_col", QueryBuilder.literal(today))
            .build());

    ResultSet rs =
        session.execute(
            "SELECT CAST(date_col AS text) AS casted_value FROM "
                + TABLE_NAME
                + " WHERE id = "
                + id);
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getString("casted_value")).isEqualTo("2025-06-10");
  }
}

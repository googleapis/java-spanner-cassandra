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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class LwtIT extends AbstractIT {

  private static final String TABLE_NAME = "unique_items";

  public LwtIT(DatabaseContext db) {
    super(db);
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("id", new ColumnDefinition("STRING(MAX)", "UUID", true));
    testColumns.put("name", new ColumnDefinition("STRING(MAX)", "text", false));
    testColumns.put("value", new ColumnDefinition("INT64", "INT", false));
    TableDefinition unique_items = new TableDefinition(TABLE_NAME, testColumns);
    db.createTables(unique_items);
  }

  protected int countRows(String tableName) {
    ResultSet rs = db.getSession().execute("SELECT COUNT(*) FROM " + tableName);
    Row row = rs.one();
    return (int) row.getLong(0);
  }

  @Test
  public void shouldInsertIfNotExists() {
    CqlSession session = db.getSession();
    UUID itemId = UUID.randomUUID();
    String itemName = "Unique Item 1";
    int itemValue = 100;

    // When - First insert (should succeed)
    ResultSet rs1 =
        session.execute(
            insertInto(TABLE_NAME)
                .value("id", QueryBuilder.literal(itemId))
                .value("name", QueryBuilder.literal(itemName))
                .value("value", QueryBuilder.literal(itemValue))
                .ifNotExists() // LWT condition
                .build());
    // Then
    assertThat(rs1.wasApplied()).isTrue();
    assertThat(countRows(TABLE_NAME)).isEqualTo(1);

    // When - Second insert with the same ID (should fail to apply)
    ResultSet rs2 =
        session.execute(
            insertInto(TABLE_NAME)
                .value("id", QueryBuilder.literal(itemId))
                .value("name", QueryBuilder.literal("Another Name")) // Different data
                .value("value", QueryBuilder.literal(200))
                .ifNotExists() // LWT condition
                .build());

    // Then
    assertThat(rs2.wasApplied()).isFalse();
    assertThat(countRows(TABLE_NAME)).isEqualTo(1);

    // Verify the original data is still there
    Row row =
        session
            .execute(
                selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id")
                    .isEqualTo(QueryBuilder.literal(itemId))
                    .build())
            .one();
    assertThat(row).isNotNull();
    assertThat(itemName).isEqualTo(row.getString("name"));
    assertThat(itemValue).isEqualTo(row.getInt("value"));
  }

  @Test
  public void shouldUpdateIfConditionMatches() {
    CqlSession session = db.getSession();
    UUID itemId = UUID.randomUUID();
    String initialName = "Item A";
    int initialValue = 50;

    // Insert initial data
    session.execute(
        insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(itemId))
            .value("name", QueryBuilder.literal(initialName))
            .value("value", QueryBuilder.literal(initialValue))
            .build());

    // When - Update where condition matches
    String newName = "Updated Item A";
    int newValue = 75;
    ResultSet rs1 =
        session.execute(
            QueryBuilder.update(TABLE_NAME)
                .setColumn("name", QueryBuilder.literal(newName))
                .setColumn("value", QueryBuilder.literal(newValue))
                .whereColumn("id")
                .isEqualTo(QueryBuilder.literal(itemId))
                .ifColumn("name")
                .isEqualTo(QueryBuilder.literal(initialName)) // LWT condition
                .build());

    // Then
    assertThat(rs1.wasApplied()).isTrue();
    Row row1 =
        session
            .execute(
                selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id")
                    .isEqualTo(QueryBuilder.literal(itemId))
                    .build())
            .one();
    assertThat(row1).isNotNull();
    assertThat(newName).isEqualTo(row1.getString("name"));
    assertThat(newValue).isEqualTo(row1.getInt("value"));

    // When - Update where condition does NOT match
    String furtherUpdatedName = "Further Updated Item A";
    ResultSet rs2 =
        session.execute(
            QueryBuilder.update(TABLE_NAME)
                .setColumn("name", QueryBuilder.literal(furtherUpdatedName))
                .whereColumn("id")
                .isEqualTo(QueryBuilder.literal(itemId))
                .ifColumn("name")
                .isEqualTo(QueryBuilder.literal(initialName)) // Condition on old value
                .build());

    // Then
    assertThat(rs2.wasApplied()).isFalse();
    // Verify data remains the same as after the first successful update
    Row row2 =
        session
            .execute(
                selectFrom(TABLE_NAME)
                    .all()
                    .whereColumn("id")
                    .isEqualTo(QueryBuilder.literal(itemId))
                    .build())
            .one();
    assertThat(row2).isNotNull();
    assertThat(newName).isEqualTo(row2.getString("name")); // Still newName, not furtherUpdatedName
    assertThat(newValue).isEqualTo(row2.getInt("value"));
  }

  @Test
  public void shouldDeleteIfConditionExists() {
    CqlSession session = db.getSession();
    UUID itemId = UUID.randomUUID();
    String itemName = "Item to Delete";
    int itemValue = 99;

    // Insert initial data
    session.execute(
        insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(itemId))
            .value("name", QueryBuilder.literal(itemName))
            .value("value", QueryBuilder.literal(itemValue))
            .build());

    assertThat(countRows(TABLE_NAME)).isEqualTo(1);

    // When - Delete where condition EXISTS (should succeed)
    ResultSet rs1 =
        session.execute(
            QueryBuilder.deleteFrom(TABLE_NAME)
                .whereColumn("id")
                .isEqualTo(QueryBuilder.literal(itemId))
                .ifExists() // LWT condition
                .build());

    // Then
    assertThat(rs1.wasApplied()).isTrue();
    assertThat(countRows(TABLE_NAME)).isEqualTo(0);

    // When - Second delete where condition EXISTS (should fail)
    ResultSet rs2 =
        session.execute(
            QueryBuilder.deleteFrom(TABLE_NAME)
                .whereColumn("id")
                .isEqualTo(QueryBuilder.literal(itemId))
                .ifExists() // LWT condition
                .build());

    // Then
    assertThat(rs2.wasApplied()).isFalse();
    assertThat(countRows(TABLE_NAME)).isEqualTo(0);
  }

  @Test
  public void shouldDeleteIfColumnMatches() {
    CqlSession session = db.getSession();
    UUID itemId = UUID.randomUUID();
    String itemName = "Item to Conditional Delete";
    int itemValue = 123;

    session.execute(
        insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(itemId))
            .value("name", QueryBuilder.literal(itemName))
            .value("value", QueryBuilder.literal(itemValue))
            .build());

    assertThat(countRows(TABLE_NAME)).isEqualTo(1);

    // When - Delete where condition matches (should succeed)
    ResultSet rs1 =
        session.execute(
            QueryBuilder.deleteFrom(TABLE_NAME)
                .whereColumn("id")
                .isEqualTo(QueryBuilder.literal(itemId))
                .ifColumn("value")
                .isEqualTo(QueryBuilder.literal(itemValue)) // LWT condition
                .build());

    // Then
    assertThat(rs1.wasApplied()).isTrue();
    assertThat(countRows(TABLE_NAME)).isEqualTo(0);

    // When - Insert it back with different value
    session.execute(
        insertInto(TABLE_NAME)
            .value("id", QueryBuilder.literal(itemId))
            .value("name", QueryBuilder.literal(itemName))
            .value("value", QueryBuilder.literal(456))
            .build());
    assertThat(countRows(TABLE_NAME)).isEqualTo(1);

    // When - Delete where condition does NOT match
    ResultSet rs2 =
        session.execute(
            QueryBuilder.deleteFrom(TABLE_NAME)
                .whereColumn("id")
                .isEqualTo(QueryBuilder.literal(itemId))
                .ifColumn("value")
                .isEqualTo(QueryBuilder.literal(itemValue)) // Condition on old value
                .build());

    // Then
    assertThat(rs2.wasApplied()).isFalse();
    assertThat(countRows(TABLE_NAME)).isEqualTo(1);
  }
}

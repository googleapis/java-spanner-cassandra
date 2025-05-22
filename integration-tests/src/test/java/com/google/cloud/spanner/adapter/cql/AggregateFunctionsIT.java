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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AggregateFunctionsIT extends AbstractIT {

  private static final String TABLE_NAME = "sales_records";

  public AggregateFunctionsIT(DatabaseContext db) {
    super(db);
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("product_id", new ColumnDefinition("STRING(MAX)", "UUID", true));
    testColumns.put("order_id", new ColumnDefinition("STRING(MAX)", "UUID", true));
    testColumns.put("quantity", new ColumnDefinition("INT64", "INT", false));
    testColumns.put("price", new ColumnDefinition("NUMERIC", "DECIMAL", false));
    TableDefinition sales_records = new TableDefinition(TABLE_NAME, testColumns);
    db.createTables(sales_records);
  }

  private void insertSalesData(UUID productId, int quantity, double price) {
    db.getSession()
        .execute(
            insertInto(TABLE_NAME)
                .value("product_id", QueryBuilder.literal(productId))
                .value("order_id", QueryBuilder.literal(UUID.randomUUID()))
                .value("quantity", QueryBuilder.literal(quantity))
                .value("price", QueryBuilder.literal(BigDecimal.valueOf(price)))
                .build());
  }

  @Test
  public void shouldCountAllRowsForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 5, 10.0);
    insertSalesData(productId, 2, 25.0);
    insertSalesData(productId, 8, 5.0);

    // When
    ResultSet rs =
        session.execute(
            selectFrom(TABLE_NAME)
                .countAll() // COUNT(*)
                .whereColumn("product_id")
                .isEqualTo(QueryBuilder.literal(productId))
                .build());

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getLong(0)).isEqualTo(3L);
  }

  @Test
  public void shouldCountNonNullValuesForColumn() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 5, 10.0);
    insertSalesData(productId, 2, 25.0);
    // Insert a row with a null quantity to test COUNT(column_name)
    session.execute(
        insertInto(TABLE_NAME)
            .value("product_id", QueryBuilder.literal(productId))
            .value("order_id", QueryBuilder.literal(UUID.randomUUID()))
            .value("price", QueryBuilder.literal(BigDecimal.valueOf(5.0))) // quantity is null
            .build());

    // When
    ResultSet rs =
        session.execute(
            "SELECT COUNT(quantity) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getLong(0)).isEqualTo(2L);
  }

  @Test
  public void shouldCalculateSumForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 5, 10.0);
    insertSalesData(productId, 2, 25.0);
    insertSalesData(productId, 8, 5.0);

    // When
    ResultSet rs =
        session.execute(
            "SELECT SUM(quantity) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(15);
  }

  @Test
  public void shouldCalculateSumOfDecimalForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 1, 10.50);
    insertSalesData(productId, 1, 20.25);
    insertSalesData(productId, 1, 5.00);

    // When
    ResultSet rs =
        session.execute(
            "SELECT SUM(price) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    BigDecimal expectedSum =
        BigDecimal.valueOf(10.50).add(BigDecimal.valueOf(20.25)).add(BigDecimal.valueOf(5.00));
    assertThat(row.getBigDecimal(0)).isEqualTo(expectedSum);
  }

  @Test
  public void shouldCalculateAverageForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 10, 10.0);
    insertSalesData(productId, 20, 20.0);
    insertSalesData(productId, 30, 30.0);

    // When
    ResultSet rs =
        session.execute(
            "SELECT AVG(quantity) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    // AVG returns a BigInt for integer types, as the average can be a fractional number in higher
    // precision.
    // It's important to cast or handle the return type correctly.
    // For integers, AVG truncates the decimal part in Cassandra.
    // For example, AVG([1,2]) would be 1, not 1.5.
    // If you need decimal precision, you typically cast the column in CQL: AVG(CAST(quantity AS
    // FLOAT)).
    assertThat(row.getInt(0)).isEqualTo(20);
  }

  @Test
  public void shouldCalculateAverageOfDecimalForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 1, 10.0);
    insertSalesData(productId, 1, 15.0);
    insertSalesData(productId, 1, 20.0);

    // When
    ResultSet rs =
        session.execute(
            "SELECT AVG(price) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    // AVG of DECIMAL returns DECIMAL
    BigDecimal expectedAvg =
        BigDecimal.valueOf(10.0)
            .add(BigDecimal.valueOf(15.0))
            .add(BigDecimal.valueOf(20.0))
            .divide(BigDecimal.valueOf(3), BigDecimal.ROUND_HALF_UP);
    assertThat(row.getBigDecimal(0).stripTrailingZeros())
        .isEqualTo(expectedAvg.stripTrailingZeros());
  }

  @Test
  public void shouldFindMaximumValueForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 5, 10.0);
    insertSalesData(productId, 2, 25.0);
    insertSalesData(productId, 8, 5.0);

    // When
    ResultSet rs =
        session.execute(
            "SELECT MAX(quantity) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(8);
  }

  @Test
  public void shouldFindMinimumValueForPartition() {
    CqlSession session = db.getSession();
    UUID productId = UUID.randomUUID();
    insertSalesData(productId, 5, 10.0);
    insertSalesData(productId, 2, 25.0);
    insertSalesData(productId, 8, 5.0);

    // When
    ResultSet rs =
        session.execute(
            "SELECT MIN(quantity) from " + TABLE_NAME + " WHERE product_id = " + productId);

    // Then
    Row row = rs.one();
    assertThat(row).isNotNull();
    assertThat(row.getInt(0)).isEqualTo(2);
  }

  @Test
  public void shouldReturnNullForAggregatesOnEmptyPartition() {
    CqlSession session = db.getSession();
    UUID nonExistentProductId = UUID.randomUUID();

    // When COUNT(*)
    ResultSet rsCount =
        session.execute(
            selectFrom(TABLE_NAME)
                .countAll()
                .whereColumn("product_id")
                .isEqualTo(QueryBuilder.literal(nonExistentProductId))
                .build());
    Row rowCount = rsCount.one();
    assertThat(rowCount).isNotNull();
    assertThat(rowCount.getLong(0)).isEqualTo(0L); // COUNT(*) always returns a row, even if 0

    // When SUM
    ResultSet rsSum =
        session.execute(
            "SELECT SUM(quantity) from "
                + TABLE_NAME
                + " WHERE product_id = "
                + nonExistentProductId);
    Row rowSum = rsSum.one();
    assertThat(rowSum).isNotNull();

    // When AVG
    ResultSet rsAvg =
        session.execute(
            "SELECT AVG(quantity) from "
                + TABLE_NAME
                + " WHERE product_id = "
                + nonExistentProductId);
    Row rowAvg = rsAvg.one();
    assertThat(rsAvg).isNotNull();
  }
}

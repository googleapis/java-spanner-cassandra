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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class BlobIT extends AbstractIT {

  private static final String TABLE_NAME = "blob_test";
  private static CqlSession session;

  private static PreparedStatement insertStatement;

  public BlobIT(DatabaseContext db) {
    super(db);
    session = db.getSession();
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("id", new ColumnDefinition("STRING(MAX)", "uuid", true));
    testColumns.put("data_blob", new ColumnDefinition("BYTES(MAX)", "blob", false));
    TableDefinition blob_test = new TableDefinition(TABLE_NAME, testColumns);
    db.createTables(blob_test);

    insertStatement =
        session.prepare("INSERT INTO " + TABLE_NAME + " (id, data_blob) VALUES (?, ?)");
  }

  private ByteBuffer generateRandomBlob(int size) {
    byte[] bytes = new byte[size];
    ThreadLocalRandom.current().nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  private void insertBlobData(UUID id, ByteBuffer dataBlob) {
    session.execute(
        insertInto(TABLE_NAME)
            .value("id", literal(id))
            .value("data_blob", literal(dataBlob))
            .build());
  }

  private ByteBuffer retrieveBlobData(UUID id) {
    ResultSet rs =
        session.execute(
            selectFrom(TABLE_NAME)
                .column("data_blob")
                .whereColumn("id")
                .isEqualTo(literal(id))
                .build());
    Row row = rs.one();
    if (row != null) {
      return row.getByteBuffer("data_blob");
    }
    return null;
  }

  private void assertBlobsEqual(ByteBuffer expected, ByteBuffer actual, String message) {
    if (expected == null && actual == null) {
      return; // Both null is equal
    }
    if (expected == null || actual == null) {
      Assertions.fail(message + ": One is null, the other is not.");
    }
    // Rewind buffers to ensure comparison from start
    expected.rewind();
    actual.rewind();
    assertTrue(expected.equals(actual));
  }

  @Test
  public void testInsertAndRetrieveSmallBlob() {
    UUID id = UUID.randomUUID();
    ByteBuffer originalBlob = generateRandomBlob(100);
    insertBlobData(id, originalBlob);
    ByteBuffer retrievedBlob = retrieveBlobData(id);
    assertBlobsEqual(originalBlob, retrievedBlob, "Small BLOB mismatch");
  }

  @Test
  public void testInsertAndRetrieveMediumBlob() {
    UUID id = UUID.randomUUID();
    int size = 1024 * 1024; // 1MB
    ByteBuffer originalBlob = generateRandomBlob(size);
    BoundStatement boundInsert = insertStatement.bind(id, originalBlob);
    session.execute(boundInsert);
    ByteBuffer retrievedBlob = retrieveBlobData(id);
    assertBlobsEqual(originalBlob, retrievedBlob, "Medium BLOB mismatch");
  }

  @Test
  public void testInsertAndRetrieveEmptyBlob() {
    UUID id = UUID.randomUUID();
    ByteBuffer originalBlob = ByteBuffer.wrap(new byte[0]); // Empty byte array
    insertBlobData(id, originalBlob);
    ByteBuffer retrievedBlob = retrieveBlobData(id);
    assertBlobsEqual(originalBlob, retrievedBlob, "Empty BLOB mismatch");
  }

  @Test
  public void testInsertAndRetrieveNullBlob() {
    UUID id = UUID.randomUUID();
    ByteBuffer originalBlob = null; // Representing a NULL BLOB
    insertBlobData(id, originalBlob); // Driver handles null ByteBuffer correctly

    ByteBuffer retrievedBlob = retrieveBlobData(id);
    assertNull(retrievedBlob);
  }

  @Test
  public void testUpdateBlob() {
    UUID id = UUID.randomUUID();
    ByteBuffer initialBlob = generateRandomBlob(50);
    insertBlobData(id, initialBlob);
    assertBlobsEqual(initialBlob, retrieveBlobData(id), "Initial BLOB mismatch for update");

    ByteBuffer updatedBlob = generateRandomBlob(200); // Different size and content
    session.execute(
        update(TABLE_NAME)
            .setColumn("data_blob", literal(updatedBlob))
            .whereColumn("id")
            .isEqualTo(literal(id))
            .build());

    ByteBuffer retrievedUpdatedBlob = retrieveBlobData(id);
    assertBlobsEqual(updatedBlob, retrievedUpdatedBlob, "Updated BLOB mismatch");
  }

  @Test
  public void testUpdateBlobToNull() {
    UUID id = UUID.randomUUID();
    ByteBuffer initialBlob = generateRandomBlob(50);
    insertBlobData(id, initialBlob);
    assertBlobsEqual(initialBlob, retrieveBlobData(id), "Initial BLOB mismatch for update to NULL");

    session.execute(
        update(TABLE_NAME)
            .setColumn("data_blob", literal(null)) // Update to NULL
            .whereColumn("id")
            .isEqualTo(literal(id))
            .build());

    ByteBuffer retrievedUpdatedBlob = retrieveBlobData(id);
    assertNull(retrievedUpdatedBlob);
  }

  @Test
  public void testDeleteBlobRow() {
    UUID id = UUID.randomUUID();
    ByteBuffer originalBlob = generateRandomBlob(75);
    insertBlobData(id, originalBlob);
    assertNotNull(retrieveBlobData(id));

    session.execute(deleteFrom(TABLE_NAME).whereColumn("id").isEqualTo(literal(id)).build());

    ByteBuffer retrievedBlobAfterDelete = retrieveBlobData(id);
    assertNull(retrievedBlobAfterDelete);
  }

  @Test
  public void testVeryLargeBlob() {
    UUID id = UUID.randomUUID();
    int size = 5 * 1024 * 1024; // 5 MB
    ByteBuffer originalBlob = generateRandomBlob(size);
    BoundStatement boundInsert = insertStatement.bind(id, originalBlob);
    session.execute(boundInsert);
    ByteBuffer retrievedBlob = retrieveBlobData(id);
    assertBlobsEqual(originalBlob, retrievedBlob, "Very large BLOB mismatch");
  }
}

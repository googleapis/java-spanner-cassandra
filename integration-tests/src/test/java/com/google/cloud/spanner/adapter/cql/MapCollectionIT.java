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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.AbstractIT;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class MapCollectionIT extends AbstractIT {

  private static final String TABLE_NAME = "user_attributes";
  private static CqlSession session;

  // Prepared Statements
  private static PreparedStatement insertStatement;
  private static PreparedStatement selectStatement;
  private static PreparedStatement updateSetEntryStatement; // For map['key'] = value
  private static PreparedStatement updateAddAllMapStatement; // For map = map + {k:v, ...}
  private static PreparedStatement updateOverwriteMapStatement; // For map = {k:v, ...}

  public MapCollectionIT(DatabaseContext db) {
    super(db);
    session = db.getSession();
  }

  @Rule public TestName name = new TestName();

  @Before
  public void createTable() throws Exception {
    Map<String, ColumnDefinition> testColumns = new HashMap<>();
    testColumns.put("user_id", new ColumnDefinition("STRING(MAX)", "uuid", true));
    testColumns.put("username", new ColumnDefinition("STRING(MAX)", "text", false));
    testColumns.put("preferences", new ColumnDefinition("JSON", "map<text, int>", false));
    testColumns.put("last_updated", new ColumnDefinition("TIMESTAMP", "timestamp", false));
    TableDefinition map_test = new TableDefinition(TABLE_NAME, testColumns);
    db.createTables(map_test);

    insertStatement =
        session.prepare(
            "INSERT INTO "
                + TABLE_NAME
                + " (user_id, username, preferences, last_updated) VALUES (?, ?, ?, ?)");
    selectStatement =
        session.prepare(
            "SELECT user_id, username, preferences, last_updated FROM "
                + TABLE_NAME
                + " WHERE user_id = ?");
    updateSetEntryStatement =
        session.prepare(
            "UPDATE " + TABLE_NAME + " SET preferences[?] = ?, last_updated = ? WHERE user_id = ?");
    updateAddAllMapStatement =
        session.prepare(
            "UPDATE " + TABLE_NAME + " SET preferences = preferences + ? WHERE user_id = ?");
    updateOverwriteMapStatement =
        session.prepare(
            "UPDATE " + TABLE_NAME + " SET preferences = ?, last_updated = ? WHERE user_id = ?");
  }

  @Test
  public void testInsertAndRetrieveMap() {
    UUID userId = RandomCqlDataGenerator.randomUUID();
    String username = RandomCqlDataGenerator.randomText(5, 10);
    Map<String, Integer> preferences = new HashMap<>();
    preferences.put("theme", 1);
    preferences.put("fontSize", 14);
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    BoundStatement boundInsert = insertStatement.bind(userId, username, preferences, now);
    session.execute(boundInsert);

    Row row = session.execute(selectStatement.bind(userId)).one();
    assertThat(row).isNotNull();
    assertThat(userId).isEqualTo(row.getUuid("user_id"));
    assertThat(username).isEqualTo(row.getString("username"));
    assertThat(now).isEqualTo(row.getInstant("last_updated"));

    Map<String, Integer> retrievedPreferences =
        row.getMap("preferences", String.class, Integer.class);
    assertNotNull(retrievedPreferences);
    assertThat(retrievedPreferences).isNotNull();
    assertThat(preferences.size()).isEqualTo(retrievedPreferences.size());
    assertThat(retrievedPreferences).isEqualTo(preferences);
  }

  @Test
  public void testUpdateSetSingleEntryInMap() {
    UUID userId = RandomCqlDataGenerator.randomUUID();
    String username = RandomCqlDataGenerator.randomText(5, 10);
    Map<String, Integer> initialPreferences = new HashMap<>();
    initialPreferences.put("volume", 75);
    initialPreferences.put("brightness", 50);
    Instant initialTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    session.execute(insertStatement.bind(userId, username, initialPreferences, initialTime));

    // Add a new entry
    String newKey = "contrast";
    Integer newValue = 60;
    Instant updateTime1 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    session.execute(updateSetEntryStatement.bind(newKey, newValue, updateTime1, userId));

    Row row = session.execute(selectStatement.bind(userId)).one();
    assertNotNull(row);
    Map<String, Integer> updatedPreferences =
        row.getMap("preferences", String.class, Integer.class);
    assertEquals(3, updatedPreferences.size());
    assertTrue(updatedPreferences.containsKey("volume"));
    assertTrue(updatedPreferences.containsKey("brightness"));
    assertTrue(updatedPreferences.containsKey(newKey));
    assertEquals(newValue, updatedPreferences.get(newKey));
    assertEquals(updateTime1, row.getInstant("last_updated"));

    // Update an existing entry
    String existingKey = "volume";
    Integer updatedValue = 90;
    Instant updateTime2 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    session.execute(updateSetEntryStatement.bind(existingKey, updatedValue, updateTime2, userId));

    row = session.execute(selectStatement.bind(userId)).one();
    assertNotNull(row);
    updatedPreferences = row.getMap("preferences", String.class, Integer.class);
    assertEquals(3, updatedPreferences.size());
    assertEquals(updatedValue, updatedPreferences.get(existingKey));
    assertEquals(updateTime2, row.getInstant("last_updated"));
  }

  @Test
  public void testUpdateAddAllToMap() {
    UUID userId = RandomCqlDataGenerator.randomUUID();
    String username = RandomCqlDataGenerator.randomText(5, 10);
    Map<String, Integer> initialPreferences = new HashMap<>();
    initialPreferences.put("settingA", 1);
    initialPreferences.put("settingB", 2);
    Instant initialTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    session.execute(insertStatement.bind(userId, username, initialPreferences, initialTime));

    Map<String, Integer> preferencesToAdd = new HashMap<>();
    preferencesToAdd.put("settingC", 3);
    preferencesToAdd.put("settingA", 11);

    session.execute(updateAddAllMapStatement.bind(preferencesToAdd, userId));

    Row row = session.execute(selectStatement.bind(userId)).one();
    assertNotNull(row);
    Map<String, Integer> retrievedPreferences =
        row.getMap("preferences", String.class, Integer.class);
    assertEquals(3, retrievedPreferences.size());
    assertEquals(Integer.valueOf(11), retrievedPreferences.get("settingA"));
    assertEquals(Integer.valueOf(2), retrievedPreferences.get("settingB"));
    assertEquals(Integer.valueOf(3), retrievedPreferences.get("settingC"));
    assertEquals(initialTime, row.getInstant("last_updated")); // last_updated should not change
  }

  @Test
  public void testUpdateOverwriteEntireMap() {
    UUID userId = RandomCqlDataGenerator.randomUUID();
    String username = RandomCqlDataGenerator.randomText(5, 10);
    Map<String, Integer> initialPreferences = new HashMap<>();
    initialPreferences.put("oldKey1", 100);
    initialPreferences.put("oldKey2", 200);
    Instant initialTime = Instant.now().minus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.MILLIS);

    session.execute(insertStatement.bind(userId, username, initialPreferences, initialTime));

    Map<String, Integer> newPreferences = new HashMap<>();
    newPreferences.put("newKeyA", 5);
    newPreferences.put("newKeyB", 15);
    Instant newTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    session.execute(updateOverwriteMapStatement.bind(newPreferences, newTime, userId));

    Row row = session.execute(selectStatement.bind(userId)).one();
    assertNotNull(row);
    Map<String, Integer> retrievedPreferences =
        row.getMap("preferences", String.class, Integer.class);
    assertEquals(newPreferences.size(), retrievedPreferences.size());
    assertThat(retrievedPreferences).isEqualTo(newPreferences);
    assertEquals(newTime, row.getInstant("last_updated")); // Verify other column also updated
  }

  @Test
  public void testInsertWithEmptyMap() {
    UUID userId = RandomCqlDataGenerator.randomUUID();
    String username = RandomCqlDataGenerator.randomText(5, 10);
    Map<String, Integer> emptyPreferences = new HashMap<>();
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    session.execute(insertStatement.bind(userId, username, emptyPreferences, now));

    Row row = session.execute(selectStatement.bind(userId)).one();
    assertNotNull(row);
    Map<String, Integer> retrievedPreferences =
        row.getMap("preferences", String.class, Integer.class);
    assertNotNull(retrievedPreferences);
    assertTrue(retrievedPreferences.isEmpty());
  }

  @Test
  public void testInsertWithNullMap() {
    UUID userId = RandomCqlDataGenerator.randomUUID();
    String username = RandomCqlDataGenerator.randomText(5, 10);
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    // Binding null for the MAP column
    BoundStatement boundInsert = insertStatement.bind(userId, username, null, now);
    session.execute(boundInsert);

    Row row = session.execute(selectStatement.bind(userId)).one();
    assertNotNull(row);
    Map<String, Integer> retrievedPreferences =
        row.getMap("preferences", String.class, Integer.class);
    assertTrue(
        "Retrieved map should be empty when null was inserted", retrievedPreferences.isEmpty());
  }
}

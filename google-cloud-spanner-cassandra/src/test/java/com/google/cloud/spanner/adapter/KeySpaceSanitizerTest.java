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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Test;

import static com.google.cloud.spanner.adapter.DriverConnectionHandler.sanitizeHyphenFromKeyspace;

public final class KeySpaceSanitizerTest {
  public KeySpaceSanitizerTest() {}

  @Test
  public void testSanitizeUseStatement() {
    String originalQuery = "USE my-app-keyspace;";
    String keyspace = "my-app-keyspace";
    String expectedQuery = "USE my_app_keyspace;";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testSanitizeUseStatementInQuotes() {
    String originalQuery = "USE \"my-app-keyspace\";";
    String keyspace = "my-app-keyspace";
    String expectedQuery = "USE \"my_app_keyspace\";";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testSanitizeSelectStatement() {
    String originalQuery = "SELECT * FROM my-app-keyspace.users;";
    String keyspace = "my-app-keyspace";
    String expectedQuery = "SELECT * FROM my_app_keyspace.users;";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testSanitizeInsertStatement() {
    String originalQuery =
        "INSERT INTO complex-ks-name.user_actions (id, action) VALUES (uuid(), 'login');";
    String keyspace = "complex-ks-name";
    String expectedQuery =
        "INSERT INTO complex_ks_name.user_actions (id, action) VALUES (uuid(), 'login');";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testSanitizeUpdateStatement() {
    String originalQuery = "UPDATE another-keyspace.settings SET value = true WHERE id = 1;";
    String keyspace = "another-keyspace";
    String expectedQuery = "UPDATE another_keyspace.settings SET value = true WHERE id = 1;";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testSanitizeDeleteStatement() {
    String originalQuery = "DELETE FROM data-store.log_entries WHERE event_time < now();";
    String keyspace = "data-store";
    String expectedQuery = "DELETE FROM data_store.log_entries WHERE event_time < now();";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldNotChangeStringLiterals() {
    String keyspace = "my-app-keyspace";
    String originalQuery =
        "INSERT INTO my_app_keyspace.logs (level, msg) VALUES ('info', 'Action related to"
            + " my-app-keyspace');";
    assertEquals(originalQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldHandleEscapedQuotesInLiterals() {
    String keyspace = "my-keyspace";
    String originalQuery =
        "UPDATE my_keyspace.docs SET content = 'This isn''t my-keyspace, it''s something else.'"
            + " WHERE id = 1;";
    assertEquals(originalQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldBeCaseInsensitive() {
    String keyspace = "my-app-keyspace";
    String originalQuery = "SELECT * FROM MY-APP-KEYSPACE.users; USE my-App-keyspace;";
    String expectedQuery = "SELECT * FROM my_app_keyspace.users; USE my_app_keyspace;";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldHandleTrickyHyphenatedKeyspace() {
    String keyspace = "h-hi";
    String originalQuery = "SELECT * FROM h-hi.some_table;";
    String expectedQuery = "SELECT * FROM h_hi.some_table;";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldNotMatchPartialNames() {
    String keyspace = "my-keyspace";
    String originalQuery = "SELECT * FROM my-keyspace-backup.users WHERE name = 'value';";
    assertEquals(originalQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldHandleMultipleOccurrences() {
    String keyspace = "ks-1";
    String originalQuery = "USE ks-1; SELECT * FROM ks-1.table1; UPDATE ks-1.table2 SET col=1;";
    String expectedQuery = "USE ks_1; SELECT * FROM ks_1.table1; UPDATE ks_1.table2 SET col=1;";
    assertEquals(expectedQuery, sanitizeHyphenFromKeyspace(originalQuery, keyspace));
  }

  @Test
  public void testShouldReturnOriginalOnNoHyphenKeyspace() {
    String originalQuery = "USE my_keyspace;";
    assertSame(originalQuery, sanitizeHyphenFromKeyspace(originalQuery, "my_keyspace"));
  }

  @Test
  public void testShouldHandleNullAndEmpty() {
    assertEquals(sanitizeHyphenFromKeyspace(null, "ks-1"), null);
    assertEquals("", sanitizeHyphenFromKeyspace("", "ks-1"));

    String query = "SELECT * FROM ks-1.t";
    assertEquals(query, sanitizeHyphenFromKeyspace(query, null));
    assertEquals(query, sanitizeHyphenFromKeyspace(query, ""));
  }

  @Test
  public void testShouldNotSanitizeKeyspaceWithinStringLiteralContent() {
    String keyspace = "my-key-space";
    String originalQuery = "UPDATE config.table SET value = '  my-key-space  ' WHERE id = 1;";
    String sanitized = sanitizeHyphenFromKeyspace(originalQuery, keyspace);
    assertEquals(originalQuery, sanitized);
  }

  @Test
  public void testShouldNotSanitizeDoubleQuotedKeyspaceWithinStringLiteralContent() {
    String keyspace = "my-key-space";
    String originalQuery = "UPDATE config.table SET value = '\"my-key-space\"' WHERE id = 1;";
    String sanitized = sanitizeHyphenFromKeyspace(originalQuery, keyspace);
    assertEquals(originalQuery, sanitized);
  }
}

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

package com.google.cloud.spanner.adapter;

import static com.google.common.truth.Truth.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.adapter.utils.ColumnDefinition;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.TableDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class MultithreadedIT extends AbstractIT {

  public MultithreadedIT(DatabaseContext db) {
    super(db);
  }

  @Test
  public void multiThreadedInsertRead() throws Exception {
    // Create table
    Map<String, ColumnDefinition> columnDefs = new HashMap<>();
    columnDefs.put("id", new ColumnDefinition("INT64", "INT", true));
    columnDefs.put("active", new ColumnDefinition("BOOL", "BOOLEAN", false));
    columnDefs.put("username", new ColumnDefinition("STRING(MAX)", "TEXT", false));
    db.createTables(new TableDefinition("users_multithreaded", columnDefs));

    final CqlSession session = db.getSession();
    final int threadCount = 1024;
    final CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      new Thread(
              () -> {
                try {
                  int userId = new Random().nextInt(Integer.MAX_VALUE);
                  String username = "User-" + index;

                  // Insert
                  session.execute(
                      "INSERT INTO users_multithreaded (id, active, username) VALUES (?, ?, ?)",
                      userId,
                      index % 2 == 0,
                      username);

                  // Read
                  ResultSet rs =
                      session.execute(
                          "SELECT id, active, username FROM users_multithreaded WHERE id = ?",
                          userId);
                  Row row = rs.one();

                  assertThat(row).isNotNull();
                  assertThat(row.getInt("id")).isEqualTo(userId);
                  assertThat(row.getBoolean("active")).isEqualTo(index % 2 == 0);
                  assertThat(row.getString("username")).isEqualTo(username);
                } finally {
                  latch.countDown();
                }
              })
          .start();
    }

    // Wait for all threads to complete
    latch.await(30, TimeUnit.SECONDS);
  }
}

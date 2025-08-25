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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Extension of BasicIT with multiple threads. */
public class MultithreadedIT extends AbstractIT {

  private static final int NUM_THREADS = 1024;
  private static final int TIMEOUT_MINUTES = 5;

  public MultithreadedIT(DatabaseContext db) {
    super(db);
  }

  @Test
  public void basicMultiThreadedTest() throws Exception {
    // 1. Create the table schema.
    Map<String, ColumnDefinition> columnDefs = new HashMap<>();
    columnDefs.put("id", new ColumnDefinition("INT64", "INT", true));
    columnDefs.put("active", new ColumnDefinition("BOOL", "BOOLEAN", false));
    columnDefs.put("username", new ColumnDefinition("STRING(MAX)", "TEXT", false));
    db.createTables(new TableDefinition("users", columnDefs));

    CqlSession session = db.getSession();

    // 2. Set up a thread pool to run tasks concurrently.
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<?>> futures = new ArrayList<>();

    // 3. Submit a task for each thread.
    for (int i = 0; i < NUM_THREADS; i++) {
      final int userId = i;
      final String username = "user" + userId;

      // Each task inserts a unique row, reads it back and then verifies it.
      futures.add(
          executor.submit(
              () -> {
                // Insert a unique row.
                session.execute(
                    "INSERT INTO users (id, active, username) VALUES (?, ?, ?)",
                    userId,
                    true,
                    username);

                // Read the data back.
                ResultSet rs =
                    session.execute("SELECT id, active, username FROM users WHERE id = ?", userId);
                Row row = rs.one();

                // Verify the data is correct.
                assertThat(row).isNotNull();
                assertThat(row.getInt("id")).isEqualTo(userId);
                assertThat(row.getBoolean("active")).isTrue();
                assertThat(row.getString("username")).isEqualTo(username);
              }));
    }

    // 4. Shut down the executor and wait for all tasks to complete.
    executor.shutdown();
    // Fail the test if it times out.
    assertThat(executor.awaitTermination(TIMEOUT_MINUTES, TimeUnit.MINUTES)).isTrue();

    // 5. Check for exceptions from any of the threads.
    // Future.get() will re-throw any exception that occurred during the task's execution.
    for (Future<?> future : futures) {
      future.get();
    }
  }
}

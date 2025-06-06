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

import com.google.cloud.spanner.adapter.utils.CassandraContext;
import com.google.cloud.spanner.adapter.utils.DatabaseContext;
import com.google.cloud.spanner.adapter.utils.SpannerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.AfterParam;
import org.junit.runners.Parameterized.BeforeParam;

/**
 * Base class for integration tests with Spanner Cassandra endpoint as well as native Cassandra. All
 * integration tests must extend this class.
 */
@RunWith(Parameterized.class)
public abstract class AbstractIT {

  protected final DatabaseContext db;

  @BeforeParam
  public static void setup(DatabaseContext db) throws Exception {
    db.initialize();
  }

  @AfterParam
  public static void tearDown(DatabaseContext db) throws Exception {
    db.cleanup();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> databases() {
    final String dbToRun = System.getProperty("backend", "both").toLowerCase();

    List<Object[]> contexts = new ArrayList<>();
    if (dbToRun.equalsIgnoreCase("spanner") || dbToRun.equalsIgnoreCase("both")) {
      contexts.add(new Object[] {new SpannerContext()});
    }
    if (dbToRun.equalsIgnoreCase("cassandra") || dbToRun.equalsIgnoreCase("both")) {
      contexts.add(new Object[] {new CassandraContext()});
    }
    return contexts;
  }

  public AbstractIT(DatabaseContext db) {
    this.db = db;
  }
}

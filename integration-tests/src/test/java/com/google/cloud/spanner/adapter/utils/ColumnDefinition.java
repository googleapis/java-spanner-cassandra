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

package com.google.cloud.spanner.adapter.utils;

/**
 * Represents the definition of a database column. This is used to generate DDLs for the
 * corresponding databases.
 */
public class ColumnDefinition {
  /**
   * The data type of the column as defined in Google Cloud Spanner (e.g., "INT64", "STRING(MAX)",
   * "BOOL").
   */
  public String spannerType;

  /**
   * The corresponding data type of the column as defined in Apache Cassandra (e.g., "int", "text",
   * "boolean").
   */
  public String cassandraType;

  /** Indicates whether this column is part of the primary key */
  public boolean primaryKey;

  public ColumnDefinition(String spannerType, String cassandraType, boolean primaryKey) {
    this.spannerType = spannerType;
    this.cassandraType = cassandraType;
    this.primaryKey = primaryKey;
  }
}

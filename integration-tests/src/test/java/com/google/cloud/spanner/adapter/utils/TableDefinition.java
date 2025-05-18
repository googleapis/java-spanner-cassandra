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

import java.util.Map;

/**
 * Represents the definition for a single table within a database, encapsulating the table's name
 * and its column definitions.
 *
 * @see ColumnDefinition
 */
public class TableDefinition {

  /** Name of the table */
  public final String tableName;

  /**
   * A map defining the columns of this table. The keys of the map are the column names, and the
   * values are {@link ColumnDefinition} objects that describe each column's properties.
   */
  public final Map<String, ColumnDefinition> columnDefinitions;

  public TableDefinition(String tableName, Map<String, ColumnDefinition> columnDefinitions) {
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions;
  }
}

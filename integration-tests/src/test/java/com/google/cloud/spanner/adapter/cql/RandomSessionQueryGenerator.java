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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RandomSessionQueryGenerator {

  private final CqlSession session;
  private final String keyspaceName;
  private final String tableName;
  private final Map<String, PreparedStatement> preparedStatementCache = new ConcurrentHashMap<>();

  public RandomSessionQueryGenerator(CqlSession session, String keyspaceName, String tableName) {
    this.session = session;
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
  }

  // --- Query Generation Methods ---

  /**
   * Generates a random INSERT query for the user_sessions table. Returns the BoundStatement and the
   * generated SessionData (for verification).
   */
  public SessionDataAndStatement generateRandomInsert() {
    String sessionId = RandomCqlDataGenerator.randomText(5, 10);
    String userId = RandomCqlDataGenerator.randomText(5, 10);
    Instant startTime =
        RandomCqlDataGenerator.randomInstant(-365)
            .truncatedTo(ChronoUnit.SECONDS); // Past year, to second precision
    Instant endTime =
        startTime.plusSeconds(RandomCqlDataGenerator.random.nextInt(3600)); // Up to 1 hour later
    int durationMinutes = (int) ChronoUnit.MINUTES.between(startTime, endTime);
    String deviceType = RandomCqlDataGenerator.randomText(5, 10);
    boolean isActive = RandomCqlDataGenerator.randomBoolean();
    Map<String, String> sessionProperties =
        RandomCqlDataGenerator.randomMap(
            0,
            3,
            () -> RandomCqlDataGenerator.randomText(5),
            () -> RandomCqlDataGenerator.randomText(10, 20));
    Set<String> tags =
        RandomCqlDataGenerator.randomSet(0, 5, () -> RandomCqlDataGenerator.randomText(3, 8));

    String query =
        "INSERT INTO "
            + tableName
            + " (session_id, user_id, start_time, end_time, duration_minutes, device_type,"
            + " is_active, session_properties, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    PreparedStatement ps = preparedStatementCache.computeIfAbsent(query, session::prepare);
    BoundStatement bs =
        ps.bind(
            sessionId,
            userId,
            startTime,
            endTime,
            durationMinutes,
            deviceType,
            isActive,
            sessionProperties,
            tags);

    return new SessionDataAndStatement(
        new SessionData(
            sessionId,
            userId,
            startTime,
            endTime,
            durationMinutes,
            deviceType,
            isActive,
            sessionProperties,
            tags),
        bs);
  }

  /** Generates a SELECT query by session_id. */
  public BoundStatement generateSelectBySessionId(String sessionId) {
    String query = "select * from " + tableName + " WHERE session_id = ?";

    PreparedStatement ps = preparedStatementCache.computeIfAbsent(query, session::prepare);
    return ps.bind(sessionId);
  }

  // --- Helper class to return generated data along with the statement ---
  public static class SessionData {
    public final String sessionId;
    public final String userId;
    public final Instant startTime;
    public final Instant endTime;
    public final int durationMinutes;
    public final String deviceType;
    public final boolean isActive;
    public final Map<String, String> sessionProperties;
    public final Set<String> tags;

    public SessionData(
        String sessionId,
        String userId,
        Instant startTime,
        Instant endTime,
        int durationMinutes,
        String deviceType,
        boolean isActive,
        Map<String, String> sessionProperties,
        Set<String> tags) {
      this.sessionId = sessionId;
      this.userId = userId;
      this.startTime = startTime;
      this.endTime = endTime;
      this.durationMinutes = durationMinutes;
      this.deviceType = deviceType;
      this.isActive = isActive;
      this.sessionProperties = sessionProperties;
      this.tags = tags;
    }

    // Add equals/hashCode for verification
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SessionData that = (SessionData) o;
      return durationMinutes == that.durationMinutes
          && isActive == that.isActive
          && Objects.equals(sessionId, that.sessionId)
          && Objects.equals(userId, that.userId)
          && Objects.equals(startTime, that.startTime)
          && Objects.equals(endTime, that.endTime)
          && Objects.equals(deviceType, that.deviceType)
          && Objects.equals(sessionProperties, that.sessionProperties)
          && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          sessionId,
          userId,
          startTime,
          endTime,
          durationMinutes,
          deviceType,
          isActive,
          sessionProperties,
          tags);
    }

    // Method to map from Cassandra Row
    public static SessionData fromRow(Row row) {
      if (row == null) return null;
      return new SessionData(
          row.getString("session_id"),
          row.getString("user_id"),
          row.getInstant("start_time"),
          row.getInstant("end_time"),
          row.getInt("duration_minutes"),
          row.getString("device_type"),
          row.getBoolean("is_active"),
          row.getMap("session_properties", String.class, String.class),
          row.getSet("tags", String.class));
    }
  }

  public static class SessionDataAndStatement {
    public final SessionData sessionData;
    public final BoundStatement statement;

    public SessionDataAndStatement(SessionData sessionData, BoundStatement statement) {
      this.sessionData = sessionData;
      this.statement = statement;
    }
  }
}

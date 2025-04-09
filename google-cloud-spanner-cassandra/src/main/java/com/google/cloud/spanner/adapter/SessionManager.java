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

import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.CreateSessionRequest;
import com.google.spanner.adapter.v1.Session;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a single Spanner Session lifecycle, providing thread-safe access and automatic refresh
 * based on a time threshold using double-checked locking.
 *
 * <p>This class is intended to be thread-safe.
 */
final class SessionManager {

  static final Duration DEFAULT_SESSION_REFRESH_THRESHOLD = Duration.ofDays(6);

  private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);

  private final AdapterClient adapterClient;
  private final String databaseUri;
  private final Clock clock;

  // Helper class to hold the session and its refresh time together atomically.
  private static class SessionHolder {
    final Session session;
    final Instant refreshTime;

    SessionHolder(Session session, Instant refreshTime) {
      this.session = session;
      this.refreshTime = refreshTime;
    }

    boolean isExpired(Instant now, Duration threshold) {
      return now.isAfter(refreshTime.plus(threshold));
    }
  }

  private volatile SessionHolder currentSessionHolder;
  private final Object sessionLock = new Object();

  /**
   * Creates a session manager with a specific refresh threshold, using the system default UTC
   * clock.
   *
   * @param adapterClient The gRPC client stub used to create sessions.
   * @param databaseUri The URI string of the Spanner database.
   */
  SessionManager(AdapterClient adapterClient, String databaseUri) {
    this(adapterClient, databaseUri, Clock.systemUTC());
  }

  /**
   * Creates a session manager with explicit configuration for refresh threshold and clock. This
   * constructor is particularly useful for testing, allowing injection of a controllable clock.
   *
   * @param adapterClient The gRPC client stub used to create sessions.
   * @param databaseUri The URI of the Spanner database.
   * @param clock The clock instance used to determine the current time for expiration checks.
   *     Allows injecting custom clocks (e.g., {@code Clock.fixed()}) for testing.
   */
  SessionManager(AdapterClient adapterClient, String databaseUri, Clock clock) {
    this.adapterClient = adapterClient;
    this.databaseUri = databaseUri;
    this.clock = clock;
    this.currentSessionHolder = null;
  }

  /**
   * Gets the current valid session, potentially refreshing it if it's null or expired. This method
   * is thread-safe and uses double-checked locking for efficiency.
   *
   * @return The current valid Session.
   * @throws SessionCreationException if creating a new session fails.
   */
  Session getSession() throws SessionCreationException {
    SessionHolder holder = currentSessionHolder; // Read volatile once
    Instant now = Instant.now(clock);

    // First check (no lock) - optimistic path
    if (holder != null && !holder.isExpired(now, DEFAULT_SESSION_REFRESH_THRESHOLD)) {
      return holder.session;
    }

    // Lock only if refresh might be needed
    synchronized (sessionLock) {
      // Second check (inside lock) - verify condition again
      holder = currentSessionHolder; // Re-read volatile inside lock
      now = Instant.now(clock); // Re-capture time

      if (holder == null || holder.isExpired(now, DEFAULT_SESSION_REFRESH_THRESHOLD)) {
        LOG.info("Refreshing Spanner session for " + databaseUri);
        try {
          Session newSession = createNewSession(); // Perform gRPC call
          // Update the volatile holder reference atomically
          this.currentSessionHolder = new SessionHolder(newSession, now);
          return newSession;
        } catch (RuntimeException e) {
          throw new SessionCreationException(
              "Failed to create Spanner session for " + databaseUri, e);
        }
      } else {
        // Another thread refreshed it while waiting for the lock
        return holder.session;
      }
    }
  }

  private Session createNewSession() {
    CreateSessionRequest request =
        CreateSessionRequest.newBuilder()
            .setParent(this.databaseUri)
            .setSession(Session.newBuilder().build())
            .build();
    return adapterClient.createSession(request);
  }

  static final class SessionCreationException extends RuntimeException {
    SessionCreationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}

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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.CreateSessionRequest;
import com.google.spanner.adapter.v1.Session;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import org.junit.Before;
import org.junit.Test;

public class SessionManagerTest {

  private static final String FAKE_DB_URI = "projects/p/instances/i/databases/d";

  private AdapterClient mockAdapterClient = mock(AdapterClient.class);
  private Session mockSession1 = mock(Session.class);
  private Session mockSession2 = mock(Session.class);
  private MutableClock clock;
  private SessionManager sessionManager;

  @Before
  public void setUp() {
    clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
    sessionManager = new SessionManager(mockAdapterClient, FAKE_DB_URI, clock);

    when(mockSession1.getName()).thenReturn("session-1");
    when(mockSession2.getName()).thenReturn("session-2");
  }

  @Test
  public void getSession_subsequentCallWithinThreshold_returnsCachedSession() throws Exception {
    when(mockAdapterClient.createSession(any(CreateSessionRequest.class))).thenReturn(mockSession1);
    Session firstResult = sessionManager.getSession();

    clock.advanceBy(Duration.ofSeconds(30));
    Session secondResult = sessionManager.getSession();

    verify(mockAdapterClient, times(1)).createSession(any());
    assertThat(secondResult).isEqualTo(firstResult);
  }

  @Test
  public void getSession_callAfterThreshold_refreshesSession() throws Exception {
    when(mockAdapterClient.createSession(any(CreateSessionRequest.class)))
        .thenReturn(mockSession1) // First call
        .thenReturn(mockSession2); // Second call (after expiry)

    Session firstResult = sessionManager.getSession();
    clock.advanceBy(SessionManager.DEFAULT_SESSION_REFRESH_THRESHOLD.plusSeconds(1));
    Session secondResult = sessionManager.getSession();

    assertThat(firstResult.getName()).isEqualTo("session-1");
    assertThat(secondResult.getName()).isEqualTo("session-2");
    verify(mockAdapterClient, times(2)).createSession(any()); // Total 2 creation calls
  }

  @Test
  public void sessionCreationFails() {
    when(mockAdapterClient.createSession(any(CreateSessionRequest.class)))
        .thenThrow(new RuntimeException());

    assertThrows(RuntimeException.class, () -> sessionManager.getSession());
    verify(mockAdapterClient, times(1)).createSession(any());
  }

  @Test
  public void getSession_CreatedOnlyOnce() throws Exception {
    final int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1); // To start all threads ~simultaneously
    CountDownLatch endLatch = new CountDownLatch(numThreads);
    List<Session> results = Collections.synchronizedList(new ArrayList<>()); // Thread-safe set
    when(mockAdapterClient.createSession(any(CreateSessionRequest.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(50); // Simulate creation time
              return mockSession1;
            });

    for (int i = 0; i < numThreads; i++) {
      executor.execute(
          () -> {
            try {
              startLatch.await(); // Wait until all threads are ready
              Session s = sessionManager.getSession();
              results.add(s);
            } catch (InterruptedException e) {
              e.printStackTrace();
            } finally {
              endLatch.countDown();
            }
          });
    }
    startLatch.countDown(); // Start all threads
    boolean finished = endLatch.await(5, TimeUnit.SECONDS); // Wait for completion
    executor.shutdown();

    assertThat(finished);
    assertThat(results.size()).isEqualTo(numThreads);
    assertThat(new HashSet<>(results)).hasSize(1); // All sessions should be the same
    verify(mockAdapterClient, times(1)).createSession(any(CreateSessionRequest.class));
  }

  /** A custom clock implementation for above tests */
  private static class MutableClock extends Clock {
    private Instant currentInstant;
    private final ZoneId zoneId;

    MutableClock(Instant initialInstant, ZoneId zoneId) {
      this.currentInstant = initialInstant;
      this.zoneId = zoneId;
    }

    void advanceBy(Duration duration) {
      currentInstant = currentInstant.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return new MutableClock(currentInstant, zone);
    }

    @Override
    public Instant instant() {
      return currentInstant;
    }
  }
}

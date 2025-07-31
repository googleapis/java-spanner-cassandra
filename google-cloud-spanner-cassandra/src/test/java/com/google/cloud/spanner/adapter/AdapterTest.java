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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;
import com.google.spanner.adapter.v1.CreateSessionRequest;
import com.google.spanner.adapter.v1.Session;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public final class AdapterTest {
  private static final String TEST_HOST = "localhost";
  private static final String TEST_DATABASE_URI =
      "projects/test-project/instances/test-instance/databases/test-db";
  private static final int TEST_PORT = 12345;
  private final InetAddress inetAddress;
  private Adapter adapter;

  public AdapterTest() throws UnknownHostException {
    inetAddress = InetAddress.getByName("0.0.0.0");
  }

  @Before
  public void setUp() {
    AdapterOptions options =
        new AdapterOptions.Builder()
            .spannerEndpoint(TEST_HOST)
            .tcpPort(TEST_PORT)
            .databaseUri(TEST_DATABASE_URI)
            .inetAddress(inetAddress)
            .build();

    adapter = new Adapter(options);
  }

  @Test
  public void successfulStartStopFlow() throws Exception {

    try (MockedConstruction<ServerSocket> mockedServerSocketConstruction =
            mockConstruction(ServerSocket.class);
        MockedStatic<Executors> mockedExecutors = mockStatic(Executors.class);
        MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class);
        MockedStatic<GoogleCredentials> mockedGoogleCredentials =
            mockStatic(GoogleCredentials.class)) {
      AdapterClient mockAdapterClient = mock(AdapterClient.class);
      Session mockSession = mock(Session.class);
      mockedGoogleCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(null);
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenReturn(mockAdapterClient);
      when(mockAdapterClient.createSession(any())).thenReturn(mockSession);
      ExecutorService mockExecutor = mock(ExecutorService.class);
      mockedExecutors.when(Executors::newCachedThreadPool).thenReturn(mockExecutor);

      adapter.start();
      adapter.stop();

      verify(mockAdapterClient, times(1)).createSession(any(CreateSessionRequest.class));
      verify(mockExecutor).execute(any(Runnable.class));
      // Verify ServerSocket was constructed
      assertEquals(1, mockedServerSocketConstruction.constructed().size());
      // Verify the executor was shut down.
      verify(mockExecutor).shutdownNow();
      // Verify ServerSocket was closed.
      verify(mockedServerSocketConstruction.constructed().get(0)).close();
    }
  }

  @Test
  public void stopWithoutStart() {
    // Adapter is in the not-started state.
    assertThrows(IllegalStateException.class, adapter::stop);
  }
}

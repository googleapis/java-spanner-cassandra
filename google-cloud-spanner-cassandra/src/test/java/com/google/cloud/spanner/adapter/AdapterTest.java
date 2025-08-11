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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;
import com.google.spanner.adapter.v1.Session;

public final class AdapterTest {
  private static final String TEST_DATABASE_URI =
      "projects/test-project/instances/test-instance/databases/test-db";
  private static final int TEST_PORT = 12345;
  private final InetAddress inetAddress;
  private Adapter adapter;

  public AdapterTest() throws UnknownHostException {
    inetAddress = InetAddress.getByName("localhost");
  }

  @Before
  public void setUp() throws IOException {
    AdapterOptions options =
        new AdapterOptions.Builder()
            .spannerEndpoint("localhost:1234")
            .tcpPort(TEST_PORT)
            .databaseUri(TEST_DATABASE_URI)
            .inetAddress(inetAddress)
            .credentials(NoCredentialsProvider.create().getCredentials())
            .build();

    adapter = new Adapter(options);
  }

  private int getAvailablePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }

  @Test
  public void successfulStartStopFlow() throws Exception {
    try (MockedConstruction<ServerSocket> mockedServerSocketConstruction =
            mockConstruction(ServerSocket.class);
        MockedStatic<Executors> mockedExecutors = mockStatic(Executors.class);
        MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class);
        MockedStatic<GoogleCredentials> mockedGoogleCredentials =
            mockStatic(GoogleCredentials.class);
        MockedConstruction<SessionManager> mockedSessionManager =
            mockConstruction(
                SessionManager.class,
                (mock, context) -> {
                  when(mock.getSession()).thenReturn(mock(Session.class));
                })) {
      AdapterClient mockAdapterClient = mock(AdapterClient.class);
      mockedGoogleCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(null);
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenReturn(mockAdapterClient);
      ExecutorService mockExecutor = mock(ExecutorService.class);
      mockedExecutors.when(Executors::newCachedThreadPool).thenReturn(mockExecutor);

      adapter.start();
      adapter.stop();

      verify(mockedSessionManager.constructed().get(0), times(1)).getSession();
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

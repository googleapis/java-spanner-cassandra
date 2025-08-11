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
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import static org.mockito.Mockito.doNothing;
import org.mockito.MockitoAnnotations;

import static com.google.common.truth.Truth.assertThat;

@RunWith(JUnit4.class)
public class LauncherTest {

  @Mock private Adapter adapter;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testHealthServerReturnsOk() throws Exception {
    doNothing().when(adapter).start();
    final int port = getFreePort();
    final Launcher launcher = new Launcher(adapter);
    Thread launcherThread =
        new Thread(
            () -> {
              try {
                launcher.start(InetAddress.getLoopbackAddress(), port);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    launcherThread.start();

    // Wait for the health server to start
    HttpURLConnection connection = null;
    for (int i = 0; i < 100; i++) {
      try {
        URL url = new URL("http://localhost:" + port + "/debug/health");
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() == 200) {
          break;
        }
      } catch (IOException e) {
        // Ignore and retry.
      }
      Thread.sleep(100);
    }
    assertThat(connection).isNotNull();
    assertThat(connection.getResponseCode()).isEqualTo(200);

    launcherThread.interrupt();
    launcherThread.join();
  }

  private static int getFreePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }
}

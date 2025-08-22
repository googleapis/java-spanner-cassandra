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

import java.io.IOException;
import java.net.InetAddress;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class HealthCheckServerTest {
  private HealthCheckServer server;
  private CloseableHttpClient client;
  private String baseUri;

  @Before
  public void setUp() throws IOException {
    // Start the server on an ephemeral port (port 0) to avoid conflicts.
    server = new HealthCheckServer(InetAddress.getLoopbackAddress(), 0);
    server.start();

    // Get the actual port the server is listening on to build the request URI.
    final int port = server.getAddress().getPort();
    baseUri = "http://localhost:" + port;

    // Create a default HttpClient instance.
    client = HttpClients.createDefault();
  }

  @After
  public void tearDown() throws IOException {
    if (server != null) {
      server.stop();
    }
    if (client != null) {
      client.close(); // Close the client to release resources.
    }
  }

  @Test
  public void server_whenNotReady_returns503() throws IOException {
    final HttpGet request = new HttpGet(baseUri + "/debug/health");

    client.execute(
        request,
        response -> {
          assertThat(response.getCode()).isEqualTo(503);
          assertThat(EntityUtils.toString(response.getEntity())).isEqualTo("Service Unavailable");
          return null;
        });
  }

  @Test
  public void server_whenSetToReady_returns200() throws IOException {
    server.setReady(true);
    final HttpGet request = new HttpGet(baseUri + "/debug/health");

    client.execute(
        request,
        response -> {
          assertThat(response.getCode()).isEqualTo(200);
          assertThat(EntityUtils.toString(response.getEntity()))
              .isEqualTo("All listeners are up and running");
          return null;
        });
  }

  @Test
  public void server_whenToggledToNotReady_returns503() throws IOException {
    server.setReady(true);
    server.setReady(false);
    final HttpGet request = new HttpGet(baseUri + "/debug/health");

    client.execute(
        request,
        response -> {
          assertThat(response.getCode()).isEqualTo(503);
          return null;
        });
  }

  @Test
  public void server_withPostRequest_returns405() throws IOException {
    final HttpPost request = new HttpPost(baseUri + "/debug/health");

    client.execute(
        request,
        response -> {
          assertThat(response.getCode()).isEqualTo(405);
          return null;
        });
  }

  @Test
  public void server_withInvalidPath_returns404() throws IOException {
    final HttpGet request = new HttpGet(baseUri + "/invalid/path");

    client.execute(
        request,
        response -> {
          assertThat(response.getCode()).isEqualTo(404);
          return null;
        });
  }
}

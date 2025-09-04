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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple, lightweight HTTP server for health check monitoring.
 *
 * <p>It listens on a configured host and port, responding to {@code GET} requests on the {@code
 * /debug/health} endpoint.
 */
final class HealthCheckServer {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServer.class);
  private static final int HTTP_OK_STATUS = 200;
  private static final int HTTP_UNAVAILABLE_STATUS = 503;
  private static final int HTTP_METHOD_NOT_ALLOWED = 405;

  private final HttpServer server;
  private final AtomicBoolean isReady = new AtomicBoolean(false);

  HealthCheckServer(InetAddress address, int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(address, port), 0);
    this.server.createContext("/debug/health", new HealthCheckHandler());
  }

  void start() {
    server.start();
    LOG.info("Health check server started on {}/debug/health", server.getAddress());
  }

  void stop() {
    server.stop(0);
    LOG.info("Health check server stopped.");
  }

  InetSocketAddress getAddress() {
    return server.getAddress();
  }

  void setReady(boolean ready) {
    isReady.set(ready);
  }

  /** This handler responds to health check requests. */
  private class HealthCheckHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
        // Respond with 405 Method Not Allowed for non-GET requests
        exchange.sendResponseHeaders(HTTP_METHOD_NOT_ALLOWED, -1);
        return;
      }
      if (isReady.get()) {
        sendResponse(exchange, HTTP_OK_STATUS, "All listeners are up and running");
      } else {
        sendResponse(exchange, HTTP_UNAVAILABLE_STATUS, "Service Unavailable");
      }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response)
        throws IOException {
      byte[] responseBytes = response.getBytes(UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
      exchange.sendResponseHeaders(statusCode, responseBytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(responseBytes);
      }
    }
  }
}

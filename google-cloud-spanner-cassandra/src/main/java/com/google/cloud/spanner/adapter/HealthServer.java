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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

class HealthServer {
  private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);
  private static final int HTTP_OK_STATUS = 200;
  private static final int HTTP_UNAVAILABLE_STATUS = 503;

  private final HttpServer server;
  private final AtomicBoolean isReady = new AtomicBoolean(false);

  HealthServer(InetAddress address, int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(address, port), 0);
    this.server.createContext("/debug/health", new HealthHandler());
    this.server.setExecutor(null); // creates a default executor
  }

  void start() {
    server.start();
    LOG.info("Health server started on {}", server.getAddress());
  }

  void stop() {
    server.stop(0);
    LOG.info("Health server stopped.");
  }

  void setReady(boolean ready) {
    isReady.set(ready);
  }

  private class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (isReady.get()) {
        sendResponse(exchange, HTTP_OK_STATUS, "All listeners are up and running");
      } else {
        sendResponse(exchange, HTTP_UNAVAILABLE_STATUS, "Service Unavailable");
      }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response)
        throws IOException {
      exchange.sendResponseHeaders(statusCode, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    }
  }
}

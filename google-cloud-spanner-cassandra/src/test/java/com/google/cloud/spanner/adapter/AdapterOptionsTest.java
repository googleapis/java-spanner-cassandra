/*
Copyright 2026 Google LLC

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder.InstanceType;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;
import com.google.spanner.adapter.v1.Session;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
public class AdapterOptionsTest {

  @Test
  public void testDefaultInstanceType_isCloud() {
    AdapterOptions options = AdapterOptions.newBuilder().build();

    assertThat(options.getInstanceType()).isEqualTo(InstanceType.CLOUD);
  }

  @Test
  public void testDefaultInetAddress_isLoopback() {
    AdapterOptions options = AdapterOptions.newBuilder().build();

    assertEquals(InetAddress.getLoopbackAddress(), options.getInetAddress());
  }

  @Test
  public void testSetSpannerEndpointAndInstanceType_newWay() {
    AdapterOptions options =
        AdapterOptions.newBuilder()
            .spannerEndpoint("omni-host:15000")
            .setInstanceType(InstanceType.OMNI)
            .build();

    assertThat(options.getSpannerEndpoint()).isEqualTo("omni-host:15000");
    assertThat(options.getInstanceType()).isEqualTo(InstanceType.OMNI);
  }

  @Test
  public void testConfigLoaderContactPoint_nonLoopbackThrowsIllegalArgumentException() {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("10.0.0.1:9042"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri("projects/p/instances/i/databases/d")
            .withConfigLoader(configLoader);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
    assertTrue(exception.getMessage().contains("loopback address"));
  }

  @Test
  public void testConfigLoaderContactPoint_multipleContactPointsThrowsIllegalArgumentException() {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS,
                Arrays.asList("127.0.0.1:9042", "127.0.0.1:9043"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri("projects/p/instances/i/databases/d")
            .withConfigLoader(configLoader);

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  @Test
  public void testConfigLoaderContactPoint_validLoopbackAccepted() throws Exception {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri("projects/p/instances/i/databases/d")
            .withConfigLoader(configLoader);

    builder.checkContactPoints();

    assertEquals(InetAddress.getByName("127.0.0.1"), builder.getAdapterInetAddress());
    assertEquals(9042, builder.getAdapterPort());
  }

  @Test
  public void testNullResolvedContactPoint_throwsIllegalArgumentException() {
    EndPoint nullEndPoint =
        new EndPoint() {
          @Override
          public SocketAddress resolve() {
            return null;
          }

          @Override
          public String asMetricPrefix() {
            return "null";
          }
        };
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri("projects/p/instances/i/databases/d")
            .addContactEndPoint(nullEndPoint);

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  @Test
  public void testExplicitLoopbackPortZero_registersBoundPortInContactPoints() throws Exception {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri("projects/p/instances/i/databases/d")
            .setUsePlainText(true)
            .addContactPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

    Adapter adapter = null;
    try (MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class);
        MockedStatic<GoogleCredentials> mockedGoogleCredentials =
            mockStatic(GoogleCredentials.class)) {
      AdapterClient mockAdapterClient = mock(AdapterClient.class);
      Session mockSession = mock(Session.class);
      mockedGoogleCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(null);
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenReturn(mockAdapterClient);
      when(mockAdapterClient.createSession(any())).thenReturn(mockSession);

      builder.checkContactPoints();
      builder.createAndStartAdapter();
      adapter = builder.getAdapter();
      Set<EndPoint> contactPoints = builder.getProgrammaticContactPoints();
      assertEquals(1, contactPoints.size());
      InetSocketAddress resolved = (InetSocketAddress) contactPoints.iterator().next().resolve();
      assertTrue("Expected bound port > 0, got " + resolved.getPort(), resolved.getPort() > 0);
    } finally {
      if (adapter != null) {
        adapter.stop();
      }
    }
  }

  @Test
  public void testCustomEndPointWithFixedPort_notReplacedByCreateAndStartAdapter()
      throws Exception {
    int freePort;
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      freePort = socket.getLocalPort();
    }
    InetSocketAddress customAddr =
        new InetSocketAddress(InetAddress.getLoopbackAddress(), freePort);
    EndPoint customEndPoint =
        new EndPoint() {
          @Override
          public SocketAddress resolve() {
            return customAddr;
          }

          @Override
          public String asMetricPrefix() {
            return "custom";
          }
        };
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri("projects/p/instances/i/databases/d")
            .setUsePlainText(true)
            .addContactEndPoint(customEndPoint);

    Adapter adapter = null;
    try (MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class);
        MockedStatic<GoogleCredentials> mockedGoogleCredentials =
            mockStatic(GoogleCredentials.class)) {
      AdapterClient mockAdapterClient = mock(AdapterClient.class);
      Session mockSession = mock(Session.class);
      mockedGoogleCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(null);
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenReturn(mockAdapterClient);
      when(mockAdapterClient.createSession(any())).thenReturn(mockSession);

      builder.checkContactPoints();
      builder.createAndStartAdapter();
      adapter = builder.getAdapter();
      Set<EndPoint> contactPoints = builder.getProgrammaticContactPoints();
      assertEquals(1, contactPoints.size());
      assertSame(customEndPoint, contactPoints.iterator().next());
    } finally {
      if (adapter != null) {
        adapter.stop();
      }
    }
  }

  @Test
  public void testHealthCheckEndpoint_ipv6Loopback() throws Exception {
    HealthCheckConfig config = HealthCheckConfig.fromEndpointString("[::1]:8080");

    assertNotNull(config);
    assertEquals(InetAddress.getByName("::1"), config.getHostAddress());
    assertEquals(8080, config.getPort());
  }

  @Test
  public void testWarnIfNonLoopback_nullAndIpv6Loopback() throws Exception {
    assertTrue(Launcher.warnIfNonLoopback(null, "gRPC listener"));
    assertFalse(Launcher.warnIfNonLoopback(InetAddress.getByName("::1"), "gRPC listener"));
  }
}

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
import static org.junit.Assert.assertEquals;
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
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
public class SpannerCqlSessionBuilderTest {

  private static final String TEST_DATABASE_URI =
      "projects/test-project/instances/test-instance/databases/test-db";

  @Test
  public void testSetExperimentalHostEndpoint_deprecatedButWorks() {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
    builder.setExperimentalHostEndpoint("experimental-host:15000");

    assertThat(builder.getHost()).isEqualTo("experimental-host:15000");
    assertThat(builder.getInstanceType()).isEqualTo(InstanceType.OMNI);
  }

  @Test
  public void testSetHostAndType_newWay() {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
    builder.setHost("omni-host:15000");
    builder.setInstanceType(InstanceType.OMNI);

    assertThat(builder.getHost()).isEqualTo("omni-host:15000");
    assertThat(builder.getInstanceType()).isEqualTo(InstanceType.OMNI);
  }

  @Test
  public void testDefaultContactPoint_usesLoopbackAndEphemeralPort() throws Exception {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();

    builder.checkContactPoints();

    InetAddress inetAddress = builder.getAdapterInetAddress();
    int port = builder.getAdapterPort();

    assertThat(inetAddress).isEqualTo(InetAddress.getLoopbackAddress());
    assertThat(inetAddress.isLoopbackAddress()).isTrue();
    assertThat(port).isEqualTo(0);
  }

  @Test
  public void testExplicitLoopbackContactPoint_accepted() throws Exception {
    List<InetSocketAddress> loopbackAddresses =
        Arrays.asList(
            new InetSocketAddress("127.0.0.1", 9042),
            new InetSocketAddress("localhost", 0),
            new InetSocketAddress("::1", 0));

    for (InetSocketAddress contactPoint : loopbackAddresses) {
      SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
      builder.addContactPoint(contactPoint);

      builder.checkContactPoints();

      InetAddress resolved = builder.getAdapterInetAddress();
      int port = builder.getAdapterPort();
      assertThat(resolved).isNotNull();
      assertThat(resolved.isLoopbackAddress()).isTrue();
      assertThat(port).isEqualTo(contactPoint.getPort());
      assertThat(builder.getUserContactPoint()).isNotNull();
    }
  }

  @Test
  public void testNonLoopbackContactPoint_throwsIllegalArgumentException() throws Exception {
    List<InetSocketAddress> invalidAddresses =
        Arrays.asList(
            new InetSocketAddress("0.0.0.0", 9042),
            new InetSocketAddress("::", 9042),
            new InetSocketAddress("192.168.1.1", 9042),
            InetSocketAddress.createUnresolved("example.com", 9042));

    try (MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class)) {
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenThrow(new IllegalStateException("Adapter must not start for invalid contact point"));

      for (InetSocketAddress invalidAddress : invalidAddresses) {
        SpannerCqlSessionBuilder builder =
            new SpannerCqlSessionBuilder()
                .setDatabaseUri(TEST_DATABASE_URI)
                .setUsePlainText(true)
                .addContactPoint(invalidAddress);

        CompletionStage<SpannerCqlSession> stage = builder.buildAsync();
        ExecutionException thrown =
            assertThrows(ExecutionException.class, () -> stage.toCompletableFuture().get());
        assertThat(thrown.getCause()).isInstanceOf(IllegalArgumentException.class);
      }
    }
  }

  @Test
  public void testCreateAndStartAdapter_registersBoundPortInContactPoints() throws Exception {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .setHost("localhost:15000")
            .setUsePlainText(true);

    Adapter startedAdapter = null;
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

      startedAdapter = builder.getAdapter();
      assertThat(startedAdapter).isNotNull();
      int boundPort = startedAdapter.getBoundPort();
      assertThat(boundPort).isGreaterThan(0);
      assertThat(boundPort).isNotEqualTo(9042);

      Set<EndPoint> contactPoints = builder.getProgrammaticContactPoints();
      assertThat(contactPoints).hasSize(1);
      InetSocketAddress resolved = (InetSocketAddress) contactPoints.iterator().next().resolve();
      assertThat(resolved.getAddress()).isEqualTo(InetAddress.getLoopbackAddress());
      assertThat(resolved.getPort()).isEqualTo(boundPort);
    } finally {
      if (startedAdapter != null) {
        startedAdapter.stop();
      }
    }
  }

  @Test
  public void testBuilderReuse_bindsDistinctEphemeralPorts() throws Exception {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .setHost("localhost:15000")
            .setUsePlainText(true);

    Adapter firstAdapter = null;
    Adapter secondAdapter = null;
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
      firstAdapter = builder.getAdapter();
      int firstPort = firstAdapter.getBoundPort();
      assertThat(firstPort).isGreaterThan(0);

      builder.checkContactPoints();
      builder.createAndStartAdapter();
      secondAdapter = builder.getAdapter();
      int secondPort = secondAdapter.getBoundPort();
      assertThat(secondPort).isGreaterThan(0);
      assertThat(secondPort).isNotEqualTo(firstPort);

      Set<EndPoint> contactPoints = builder.getProgrammaticContactPoints();
      assertThat(contactPoints).hasSize(1);
      InetSocketAddress resolved = (InetSocketAddress) contactPoints.iterator().next().resolve();
      assertThat(resolved.getPort()).isEqualTo(secondPort);
    } finally {
      if (firstAdapter != null) {
        firstAdapter.stop();
      }
      if (secondAdapter != null && secondAdapter != firstAdapter) {
        secondAdapter.stop();
      }
    }
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
            .setDatabaseUri(TEST_DATABASE_URI)
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
            .setDatabaseUri(TEST_DATABASE_URI)
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
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader);

    builder.checkContactPoints();

    assertEquals(InetAddress.getByName("127.0.0.1"), builder.getAdapterInetAddress());
    assertEquals(9042, builder.getAdapterPort());
  }

  @Test
  public void testConfigLoaderContactPoint_dualStackLoopbackAcceptedAndMixedRejected()
      throws Exception {
    InetAddress ipv4Loopback = InetAddress.getByName("127.0.0.1");
    InetAddress ipv6Loopback = InetAddress.getByName("::1");
    InetAddress nonLoopback = InetAddress.getByName("10.0.0.1");

    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("multi-host:9042"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader);

    try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class)) {
      mockedInetAddress
          .when(() -> InetAddress.getAllByName("multi-host"))
          .thenReturn(new InetAddress[] {ipv4Loopback, ipv6Loopback});
      builder.checkContactPoints();
      assertEquals(ipv4Loopback, builder.getAdapterInetAddress());
      assertEquals(9042, builder.getAdapterPort());

      mockedInetAddress
          .when(() -> InetAddress.getAllByName("multi-host"))
          .thenReturn(new InetAddress[] {ipv4Loopback, nonLoopback});
      assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
    }
  }

  @Test
  public void testConfigLoaderContactPoint_portZeroThrowsIllegalArgumentException() {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:0"))
            .build();

    try (MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class)) {
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenThrow(new IllegalStateException("Adapter must not start for port 0 in config"));

      SpannerCqlSessionBuilder builder =
          new SpannerCqlSessionBuilder()
              .setDatabaseUri(TEST_DATABASE_URI)
              .setUsePlainText(true)
              .withConfigLoader(configLoader);

      CompletionStage<SpannerCqlSession> stage = builder.buildAsync();
      ExecutionException thrown =
          assertThrows(ExecutionException.class, () -> stage.toCompletableFuture().get());
      assertEquals(IllegalArgumentException.class, thrown.getCause().getClass());
    }
  }

  @Test
  public void testDefaultConfigLoaderContactPoint_nonLoopbackThrowsIllegalArgumentException() {
    String propertyKey = "datastax-java-driver.basic.contact-points.0";
    String previousValue = System.getProperty(propertyKey);
    System.setProperty(propertyKey, "192.168.1.1:9042");
    try (MockedStatic<AdapterClient> mockedStaticAdapterClient = mockStatic(AdapterClient.class)) {
      mockedStaticAdapterClient
          .when(() -> AdapterClient.create(any(AdapterSettings.class)))
          .thenThrow(new IllegalStateException("Adapter must not start for invalid contact point"));

      SpannerCqlSessionBuilder builder =
          new SpannerCqlSessionBuilder().setDatabaseUri(TEST_DATABASE_URI).setUsePlainText(true);

      CompletionStage<SpannerCqlSession> stage = builder.buildAsync();
      ExecutionException thrown =
          assertThrows(ExecutionException.class, () -> stage.toCompletableFuture().get());
      assertEquals(IllegalArgumentException.class, thrown.getCause().getClass());
    } finally {
      if (previousValue == null) {
        System.clearProperty(propertyKey);
      } else {
        System.setProperty(propertyKey, previousValue);
      }
      ConfigFactory.invalidateCaches();
    }
  }

  @Test
  public void testConfigLoaderContactPoint_notDuplicatedIntoProgrammaticContactPoints()
      throws Exception {
    int freePort;
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      freePort = socket.getLocalPort();
    }
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS,
                Collections.singletonList("127.0.0.1:" + freePort))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .setHost("localhost:15000")
            .setUsePlainText(true)
            .withConfigLoader(configLoader);

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

      assertEquals(freePort, adapter.getBoundPort());
      assertThat(builder.getProgrammaticContactPoints()).isEmpty();
    } finally {
      if (adapter != null) {
        adapter.stop();
      }
    }
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
            .setDatabaseUri(TEST_DATABASE_URI)
            .addContactEndPoint(nullEndPoint);

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  @Test
  public void testExplicitLoopbackPortZero_registersBoundPortInContactPoints() throws Exception {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
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

      // Verify builder reuse restores the original user-supplied contact point.
      builder.checkContactPoints();
      assertEquals(1, builder.getProgrammaticContactPoints().size());
      assertSame(
          builder.getUserContactPoint(), builder.getProgrammaticContactPoints().iterator().next());
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
            .setDatabaseUri(TEST_DATABASE_URI)
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
  public void testUnresolvedLoopbackContactPoint_acceptedAndUnresolvedWildcardRejected()
      throws Exception {
    List<InetSocketAddress> unresolvedLoopback =
        Arrays.asList(
            InetSocketAddress.createUnresolved("127.0.0.1", 9042),
            InetSocketAddress.createUnresolved("::1", 9042),
            InetSocketAddress.createUnresolved("localhost", 9042));

    for (InetSocketAddress contactPoint : unresolvedLoopback) {
      SpannerCqlSessionBuilder builder =
          new SpannerCqlSessionBuilder()
              .setDatabaseUri(TEST_DATABASE_URI)
              .addContactPoint(contactPoint);
      builder.checkContactPoints();
      assertThat(builder.getAdapterInetAddress()).isNotNull();
      assertThat(builder.getAdapterInetAddress().isLoopbackAddress()).isTrue();
      assertThat(builder.getAdapterPort()).isEqualTo(9042);
    }

    SpannerCqlSessionBuilder wildcardBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .addContactPoint(InetSocketAddress.createUnresolved("0.0.0.0", 9042));
    assertThrows(IllegalArgumentException.class, wildcardBuilder::checkContactPoints);
  }

  @Test
  public void testIdenticalConfigAndProgrammaticContactPoint_deduplicated() throws Exception {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"))
            .build();
    SpannerCqlSessionBuilder duplicateBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042));

    duplicateBuilder.checkContactPoints();
    assertEquals(InetAddress.getByName("127.0.0.1"), duplicateBuilder.getAdapterInetAddress());
    assertEquals(9042, duplicateBuilder.getAdapterPort());

    SpannerCqlSessionBuilder distinctBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9043));
    assertThrows(IllegalArgumentException.class, distinctBuilder::checkContactPoints);
  }

  @Test
  public void testConfigLoaderContactPoint_unbracketedAndBracketedIpv6LoopbackAccepted()
      throws Exception {
    InetAddress ipv6Loopback = InetAddress.getByName("::1");
    for (String cp : Arrays.asList("::1:9042", "[::1]:9042")) {
      DriverConfigLoader configLoader =
          DriverConfigLoader.programmaticBuilder()
              .withStringList(DefaultDriverOption.CONTACT_POINTS, Collections.singletonList(cp))
              .build();
      SpannerCqlSessionBuilder builder =
          new SpannerCqlSessionBuilder()
              .setDatabaseUri(TEST_DATABASE_URI)
              .withConfigLoader(configLoader);

      builder.checkContactPoints();
      assertEquals(ipv6Loopback, builder.getAdapterInetAddress());
      assertEquals(9042, builder.getAdapterPort());
    }

    for (String malformed :
        Arrays.asList("[::1:9042", "127.0.0.1:notaport", "127.0.0.1", ":9042")) {
      DriverConfigLoader configLoader =
          DriverConfigLoader.programmaticBuilder()
              .withStringList(
                  DefaultDriverOption.CONTACT_POINTS, Collections.singletonList(malformed))
              .build();
      SpannerCqlSessionBuilder builder =
          new SpannerCqlSessionBuilder()
              .setDatabaseUri(TEST_DATABASE_URI)
              .withConfigLoader(configLoader);
      assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
    }
  }

  @Test
  public void testDefaultConfigLoader_usesCustomClassLoader() throws Exception {
    ClassLoader loopbackConfigCl =
        classLoaderWithApplicationConf(
            "datastax-java-driver.basic.contact-points = [\"127.0.0.1:9045\"]\n");
    SpannerCqlSessionBuilder loopbackBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withClassLoader(loopbackConfigCl);

    loopbackBuilder.checkContactPoints();
    assertEquals(InetAddress.getByName("127.0.0.1"), loopbackBuilder.getAdapterInetAddress());
    assertEquals(9045, loopbackBuilder.getAdapterPort());

    ClassLoader nonLoopbackConfigCl =
        classLoaderWithApplicationConf(
            "datastax-java-driver.basic.contact-points = [\"10.0.0.1:9045\"]\n");
    SpannerCqlSessionBuilder nonLoopbackBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withClassLoader(nonLoopbackConfigCl);
    assertThrows(IllegalArgumentException.class, nonLoopbackBuilder::checkContactPoints);
  }

  private static ClassLoader classLoaderWithApplicationConf(final String content) {
    final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    return new URLClassLoader(new URL[0], SpannerCqlSessionBuilderTest.class.getClassLoader()) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        if ("application.conf".equals(name)) {
          URL url =
              new URL(
                  "memory",
                  null,
                  -1,
                  "/application.conf",
                  new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL u) {
                      return new URLConnection(u) {
                        @Override
                        public void connect() {}

                        @Override
                        public InputStream getInputStream() {
                          return new ByteArrayInputStream(bytes);
                        }
                      };
                    }
                  });
          return Collections.enumeration(Collections.singletonList(url));
        }
        return super.getResources(name);
      }
    };
  }
}

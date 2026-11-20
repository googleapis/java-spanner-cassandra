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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder.InstanceType;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;
import com.google.spanner.adapter.v1.CreateSessionRequest;
import com.google.spanner.adapter.v1.Session;
import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

  // Verifies that config contact points resolving to multiple addresses (including dual-stack
  // IPv4/IPv6 loopback or mixed non-loopback) are rejected to enforce a 1-to-1 binding.
  @Test
  public void testConfigLoaderContactPoint_multipleResolvedAddressesThrowsIllegalArgumentException()
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
      assertThrows(IllegalArgumentException.class, builder::checkContactPoints);

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

  // Verifies that when no contact point is provided, the builder marks the contact point as
  // implicit so DefaultLoadBalancingPolicy infers the local datacenter from the server.
  @Test
  public void testBuildContext_treatsDefaultContactPointAsImplicitWhenNoContactPointProvided()
      throws Exception {
    DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder().build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader);

    builder.checkContactPoints();
    InternalDriverContext context =
        (InternalDriverContext)
            builder.buildContext(configLoader, ProgrammaticArguments.builder().build());
    try {
      assertTrue(context.getMetadataManager().wasImplicitContactPoint());
      DefaultNode contactPointNode = registerSingleContactPoint(context, "datacenter1");
      Map<UUID, Node> nodes = Collections.singletonMap(UUID.randomUUID(), contactPointNode);
      Map<Node, NodeDistance> recordedDistances = new HashMap<>();

      LoadBalancingPolicy policy =
          new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
      policy.init(nodes, recordedDistances::put);

      assertEquals(NodeDistance.LOCAL, recordedDistances.get(contactPointNode));
    } finally {
      closeDriverContext(context);
    }
  }

  // Verifies that explicit local datacenter settings (both programmatic and config-supplied)
  // are preserved untouched without injecting or overriding driver config options.
  @Test
  public void testBuildContext_preservesProgrammaticAndConfigLocalDatacenter() throws Exception {
    DriverConfigLoader defaultLoader = DriverConfigLoader.programmaticBuilder().build();
    SpannerCqlSessionBuilder programmaticBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(defaultLoader)
            .withLocalDatacenter("custom-dc");
    programmaticBuilder.checkContactPoints();
    InternalDriverContext programmaticContext =
        (InternalDriverContext)
            programmaticBuilder.buildContext(
                defaultLoader,
                ProgrammaticArguments.builder()
                    .withLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME, "custom-dc")
                    .build());
    try {
      assertEquals(
          "custom-dc", programmaticContext.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME));
      assertFalse(
          programmaticContext
              .getConfig()
              .getDefaultProfile()
              .isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
    } finally {
      closeDriverContext(programmaticContext);
    }

    DriverConfigLoader configDcLoader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "config-dc")
            .build();
    SpannerCqlSessionBuilder configBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configDcLoader);
    configBuilder.checkContactPoints();
    DriverContext configContext =
        configBuilder.buildContext(configDcLoader, ProgrammaticArguments.builder().build());
    try {
      assertEquals(
          "config-dc",
          configContext
              .getConfig()
              .getDefaultProfile()
              .getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
    } finally {
      closeDriverContext((InternalDriverContext) configContext);
    }
  }

  // Verifies that an explicit programmatic contact point is NOT marked implicit, preserving the
  // standard driver requirement that an explicit local datacenter must also be supplied.
  @Test
  public void testBuildContext_doesNotMarkContactPointImplicitWhenExplicitContactPointProvided()
      throws Exception {
    DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder().build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042));

    builder.checkContactPoints();
    InternalDriverContext context =
        (InternalDriverContext)
            builder.buildContext(configLoader, ProgrammaticArguments.builder().build());
    try {
      assertFalse(context.getMetadataManager().wasImplicitContactPoint());
      assertFalse(
          context
              .getConfig()
              .getDefaultProfile()
              .isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));

      DefaultNode contactPointNode = registerSingleContactPoint(context, "datacenter1");
      Map<UUID, Node> nodes = Collections.singletonMap(UUID.randomUUID(), contactPointNode);
      LoadBalancingPolicy policy =
          new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
      assertThrows(IllegalStateException.class, () -> policy.init(nodes, (node, distance) -> {}));
    } finally {
      closeDriverContext(context);
    }
  }

  // Verifies that specifying different ports in DriverConfigLoader and addContactPoint(...) is
  // rejected because the in-process builder only connects to a single local adapter.
  @Test
  public void testContactPoints_mismatchedProgrammaticAndConfigContactPointsThrows() {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9043));

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  // Verifies that named execution profiles without an explicit local datacenter still infer the
  // server's datacenter when no contact point was supplied (a case that breaks if a fallback
  // OptionsMap only defines the "default" profile).
  @Test
  public void testBuildContext_namedProfileInheritsLocalDatacenterWhenNoContactPointProvided()
      throws Exception {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .startProfile("oltp")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
            .endProfile()
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader);

    builder.checkContactPoints();
    InternalDriverContext context =
        (InternalDriverContext)
            builder.buildContext(configLoader, ProgrammaticArguments.builder().build());
    try {
      DefaultNode contactPointNode = registerSingleContactPoint(context, "us-central1");
      Map<UUID, Node> nodes = Collections.singletonMap(UUID.randomUUID(), contactPointNode);
      Map<Node, NodeDistance> recordedDistances = new HashMap<>();

      DefaultLoadBalancingPolicy policy = new DefaultLoadBalancingPolicy(context, "oltp");
      policy.init(nodes, recordedDistances::put);

      assertEquals(NodeDistance.LOCAL, recordedDistances.get(contactPointNode));
    } finally {
      closeDriverContext(context);
    }
  }

  // Verifies that the caller's DriverConfigLoader is neither wrapped nor mutated (preserving
  // supportsReloading() and config option visibility) and that non-default server datacenters
  // are inferred rather than rejected against a hardcoded value.
  @Test
  public void testBuildContext_infersNonDefaultDatacenterFromContactPointAndPreservesConfigLoader()
      throws Exception {
    DriverConfigLoader configLoader = DriverConfigLoader.fromString("");
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader);

    builder.checkContactPoints();
    InternalDriverContext context =
        (InternalDriverContext)
            builder.buildContext(configLoader, ProgrammaticArguments.builder().build());
    try {
      assertSame(configLoader, context.getConfigLoader());
      assertFalse(context.getConfigLoader().supportsReloading());
      assertFalse(
          context
              .getConfig()
              .getDefaultProfile()
              .isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));

      DefaultNode contactPointNode = registerSingleContactPoint(context, "us-central1");
      Map<UUID, Node> nodes = Collections.singletonMap(UUID.randomUUID(), contactPointNode);
      Map<Node, NodeDistance> recordedDistances = new HashMap<>();

      LoadBalancingPolicy policy =
          new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
      policy.init(nodes, recordedDistances::put);

      assertEquals(NodeDistance.LOCAL, recordedDistances.get(contactPointNode));
    } finally {
      closeDriverContext(context);
    }
  }

  // Verifies that specifying the same loopback address and port in both DriverConfigLoader and
  // addContactPoint(...) is deduplicated and accepted instead of failing as multiple endpoints.
  @Test
  public void testContactPoints_matchingProgrammaticAndConfigContactPointAllowed()
      throws Exception {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042));

    builder.checkContactPoints();

    assertEquals(InetAddress.getByName("127.0.0.1"), builder.getAdapterInetAddress());
    assertEquals(9042, builder.getAdapterPort());
  }

  // Verifies that when no explicit DriverConfigLoader is set, checkContactPoints() loads default
  // configuration using the ClassLoader passed to withClassLoader(...) so contact points defined
  // in that ClassLoader's application.conf are honored.
  @Test
  public void testCheckContactPoints_respectsCustomClassLoader() throws Exception {
    String hocon = "datastax-java-driver.basic.contact-points = [\"127.0.0.1:9042\"]\n";
    URL resourceUrl =
        new URL(
            "memory",
            "",
            0,
            "/application.conf",
            new URLStreamHandler() {
              @Override
              protected URLConnection openConnection(URL url) {
                return new URLConnection(url) {
                  @Override
                  public void connect() {}

                  @Override
                  public InputStream getInputStream() {
                    return new ByteArrayInputStream(hocon.getBytes(StandardCharsets.UTF_8));
                  }
                };
              }
            });
    ClassLoader customClassLoader =
        new ClassLoader(SpannerCqlSessionBuilderTest.class.getClassLoader()) {
          @Override
          public URL getResource(String name) {
            if ("application.conf".equals(name)) {
              return resourceUrl;
            }
            return super.getResource(name);
          }

          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            if ("application.conf".equals(name)) {
              return Collections.enumeration(Collections.singletonList(resourceUrl));
            }
            return super.getResources(name);
          }
        };

    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withClassLoader(customClassLoader);

    builder.checkContactPoints();

    assertEquals(InetAddress.getByName("127.0.0.1"), builder.getAdapterInetAddress());
    assertEquals(9042, builder.getAdapterPort());
    assertNull(builder.getUserContactPoint());
  }

  // Verifies that if super.buildAsync() fails after createAndStartAdapter() binds its socket,
  // buildAsync() stops the started Adapter so its ServerSocket and resources do not leak.
  @Test
  public void testBuildAsync_stopsAdapterWhenSessionBuildFails() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    MethodDescriptor<CreateSessionRequest, Session> createSessionMethod =
        MethodDescriptor.<CreateSessionRequest, Session>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("google.spanner.adapter.v1.Adapter/CreateSession")
            .setRequestMarshaller(ProtoUtils.marshaller(CreateSessionRequest.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Session.getDefaultInstance()))
            .build();
    Server inProcessServer = null;
    ManagedChannel inProcessChannel = null;
    SpannerCqlSessionBuilder builder = null;
    try {
      inProcessServer =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(
                  ServerServiceDefinition.builder("google.spanner.adapter.v1.Adapter")
                      .addMethod(
                          createSessionMethod,
                          ServerCalls.asyncUnaryCall(
                              (request, responseObserver) -> {
                                responseObserver.onNext(
                                    Session.newBuilder()
                                        .setName(TEST_DATABASE_URI + "/sessions/test")
                                        .build());
                                responseObserver.onCompleted();
                              }))
                      .build())
              .build()
              .start();
      inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

      DriverConfigLoader failingConfigLoader =
          DriverConfigLoader.programmaticBuilder()
              .withString(
                  DefaultDriverOption.RETRY_POLICY_CLASS, "com.example.NonExistentRetryPolicy")
              .build();

      builder =
          new SpannerCqlSessionBuilder()
              .setDatabaseUri(TEST_DATABASE_URI)
              .setUsePlainText(true)
              .setChannelProvider(
                  FixedTransportChannelProvider.create(
                      GrpcTransportChannel.create(inProcessChannel)))
              .withConfigLoader(failingConfigLoader);

      CompletionStage<SpannerCqlSession> stage = builder.buildAsync();
      assertThrows(ExecutionException.class, () -> stage.toCompletableFuture().get());

      Adapter startedAdapter = builder.getAdapter();
      assertNotNull(startedAdapter);
      int boundPort = startedAdapter.getBoundPort();
      assertTrue(boundPort > 0);

      assertThrows(
          IOException.class,
          () -> {
            try (Socket probe = new Socket()) {
              probe.connect(
                  new InetSocketAddress(InetAddress.getLoopbackAddress(), boundPort), 1000);
            }
          });
    } finally {
      if (builder != null && builder.getAdapter() != null) {
        try {
          builder.getAdapter().stop();
        } catch (IOException | RuntimeException ignored) {
          // Already stopped by buildAsync failure cleanup or never started.
        }
      }
      if (inProcessChannel != null) {
        inProcessChannel.shutdownNow();
      }
      if (inProcessServer != null) {
        inProcessServer.shutdownNow();
      }
    }
  }

  // Verifies that an explicit contact point supplied solely via DriverConfigLoader is not marked
  // implicit in MetadataManager.
  @Test
  public void testBuildContext_configContactPointOnlyDoesNotMarkImplicit() throws Exception {
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
    InternalDriverContext context =
        (InternalDriverContext)
            builder.buildContext(configLoader, ProgrammaticArguments.builder().build());
    try {
      assertFalse(context.getMetadataManager().wasImplicitContactPoint());
    } finally {
      closeDriverContext(context);
    }
  }

  // Verifies that adding multiple programmatic contact points is rejected.
  @Test
  public void testProgrammaticContactPoints_multipleThrowsIllegalArgumentException() {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9043));

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  // Verifies that matching ports on different loopback families (IPv6 ::1 vs IPv4 127.0.0.1) are
  // treated as distinct contact points and rejected.
  @Test
  public void testContactPoints_matchingPortDifferentLoopbackAddressThrows() {
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"))
            .build();
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(configLoader)
            .addContactPoint(new InetSocketAddress("::1", 9042));

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  // Verifies that malformed config contact points (missing port or empty host) and unresolvable or
  // empty DNS results are rejected with IllegalArgumentException.
  @Test
  public void testConfigLoaderContactPoint_invalidSyntaxOrResolutionThrows() throws Exception {
    for (String invalidContactPoint : Arrays.asList("127.0.0.1", ":9042")) {
      DriverConfigLoader configLoader =
          DriverConfigLoader.programmaticBuilder()
              .withStringList(
                  DefaultDriverOption.CONTACT_POINTS,
                  Collections.singletonList(invalidContactPoint))
              .build();
      SpannerCqlSessionBuilder builder =
          new SpannerCqlSessionBuilder()
              .setDatabaseUri(TEST_DATABASE_URI)
              .withConfigLoader(configLoader);
      assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
    }

    DriverConfigLoader unresolvableLoader =
        DriverConfigLoader.programmaticBuilder()
            .withStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.singletonList("missing-host:9042"))
            .build();
    SpannerCqlSessionBuilder unresolvableBuilder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .withConfigLoader(unresolvableLoader);

    try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class)) {
      mockedInetAddress
          .when(() -> InetAddress.getAllByName("missing-host"))
          .thenThrow(new UnknownHostException("missing-host"));
      assertThrows(IllegalArgumentException.class, unresolvableBuilder::checkContactPoints);

      mockedInetAddress
          .when(() -> InetAddress.getAllByName("missing-host"))
          .thenReturn(new InetAddress[0]);
      assertThrows(IllegalArgumentException.class, unresolvableBuilder::checkContactPoints);
    }
  }

  // Verifies that a programmatic EndPoint resolving to a non-InetSocketAddress is rejected.
  @Test
  public void testProgrammaticContactPoint_unsupportedSocketAddressThrows() {
    SocketAddress customSocketAddress = new SocketAddress() {};
    EndPoint customEndPoint =
        new EndPoint() {
          @Override
          public SocketAddress resolve() {
            return customSocketAddress;
          }

          @Override
          public String asMetricPrefix() {
            return "custom";
          }
        };
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .addContactEndPoint(customEndPoint);

    assertThrows(IllegalArgumentException.class, builder::checkContactPoints);
  }

  // Verifies that stopAdapterQuietly safely handles null and swallows IOException/RuntimeException
  // from Adapter.stop() so cleanup failures do not mask the underlying session build error.
  @Test
  public void testStopAdapterQuietly_handlesNullAndStopFailure() throws Exception {
    SpannerCqlSessionBuilder.stopAdapterQuietly(null);

    Adapter failingIoAdapter = mock(Adapter.class);
    doThrow(new IOException("close failure")).when(failingIoAdapter).stop();
    SpannerCqlSessionBuilder.stopAdapterQuietly(failingIoAdapter);
    verify(failingIoAdapter).stop();

    Adapter failingRuntimeAdapter = mock(Adapter.class);
    doThrow(new IllegalStateException("not started")).when(failingRuntimeAdapter).stop();
    SpannerCqlSessionBuilder.stopAdapterQuietly(failingRuntimeAdapter);
    verify(failingRuntimeAdapter).stop();
  }

  private static DefaultNode registerSingleContactPoint(
      InternalDriverContext context, String datacenter) throws Exception {
    EndPoint endPoint =
        new DefaultEndPoint(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042));
    context.getMetadataManager().addContactPoints(Collections.singleton(endPoint));
    DefaultNode contactPointNode =
        context.getMetadataManager().getContactPoints().iterator().next();
    Field datacenterField = DefaultNode.class.getDeclaredField("datacenter");
    datacenterField.setAccessible(true);
    datacenterField.set(contactPointNode, datacenter);
    return contactPointNode;
  }

  private static void closeDriverContext(InternalDriverContext context) throws Exception {
    context.getMetadataManager().forceCloseAsync().toCompletableFuture().get(5, TimeUnit.SECONDS);
    context.getNettyOptions().onClose().get(5, TimeUnit.SECONDS);
  }
}

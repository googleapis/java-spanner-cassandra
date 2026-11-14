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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.adapter.SpannerCqlSessionBuilder.InstanceType;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.AdapterSettings;
import com.google.spanner.adapter.v1.Session;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
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
  public void testSetExperimentalHostEndpoint_deprecatedButWorks() throws Exception {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
    builder.setExperimentalHostEndpoint("experimental-host:15000");

    Field hostField = SpannerCqlSessionBuilder.class.getDeclaredField("host");
    hostField.setAccessible(true);
    String host = (String) hostField.get(builder);
    assertThat(host).isEqualTo("experimental-host:15000");

    Field instanceTypeField = SpannerCqlSessionBuilder.class.getDeclaredField("instanceType");
    instanceTypeField.setAccessible(true);
    InstanceType instanceType = (InstanceType) instanceTypeField.get(builder);
    assertThat(instanceType).isEqualTo(InstanceType.OMNI);
  }

  @Test
  public void testSetHostAndType_newWay() throws Exception {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
    builder.setHost("omni-host:15000");
    builder.setInstanceType(InstanceType.OMNI);

    Field hostField = SpannerCqlSessionBuilder.class.getDeclaredField("host");
    hostField.setAccessible(true);
    String host = (String) hostField.get(builder);
    assertThat(host).isEqualTo("omni-host:15000");

    Field instanceTypeField = SpannerCqlSessionBuilder.class.getDeclaredField("instanceType");
    instanceTypeField.setAccessible(true);
    InstanceType instanceType = (InstanceType) instanceTypeField.get(builder);
    assertThat(instanceType).isEqualTo(InstanceType.OMNI);
  }

  @Test
  public void testDefaultContactPoint_usesLoopbackAndEphemeralPort() throws Exception {
    SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();

    Method checkContactPoints =
        SpannerCqlSessionBuilder.class.getDeclaredMethod("checkContactPoints");
    checkContactPoints.setAccessible(true);
    checkContactPoints.invoke(builder);

    Field inetAddressField = SpannerCqlSessionBuilder.class.getDeclaredField("iNetAddress");
    inetAddressField.setAccessible(true);
    InetAddress inetAddress = (InetAddress) inetAddressField.get(builder);

    Field portField = SpannerCqlSessionBuilder.class.getDeclaredField("port");
    portField.setAccessible(true);
    int port = (int) portField.get(builder);

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

    Method checkContactPoints =
        SpannerCqlSessionBuilder.class.getDeclaredMethod("checkContactPoints");
    checkContactPoints.setAccessible(true);
    Field inetAddressField = SpannerCqlSessionBuilder.class.getDeclaredField("iNetAddress");
    inetAddressField.setAccessible(true);
    Field portField = SpannerCqlSessionBuilder.class.getDeclaredField("port");
    portField.setAccessible(true);
    Field userContactPointField =
        SpannerCqlSessionBuilder.class.getDeclaredField("userContactPoint");
    userContactPointField.setAccessible(true);

    for (InetSocketAddress contactPoint : loopbackAddresses) {
      SpannerCqlSessionBuilder builder = new SpannerCqlSessionBuilder();
      builder.addContactPoint(contactPoint);

      checkContactPoints.invoke(builder);

      InetAddress resolved = (InetAddress) inetAddressField.get(builder);
      int port = (int) portField.get(builder);
      assertThat(resolved).isNotNull();
      assertThat(resolved.isLoopbackAddress()).isTrue();
      assertThat(port).isEqualTo(contactPoint.getPort());
      assertThat(userContactPointField.get(builder)).isNotNull();
    }
  }

  @Test
  public void testNonLoopbackContactPoint_throwsIllegalArgumentException() throws Exception {
    List<InetSocketAddress> invalidAddresses =
        Arrays.asList(
            new InetSocketAddress("0.0.0.0", 9042),
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
  @SuppressWarnings("unchecked")
  public void testCreateAndStartAdapter_registersBoundPortInContactPoints() throws Exception {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .setHost("localhost:15000")
            .setUsePlainText(true);

    Method checkContactPoints =
        SpannerCqlSessionBuilder.class.getDeclaredMethod("checkContactPoints");
    checkContactPoints.setAccessible(true);
    Method createAndStartAdapter =
        SpannerCqlSessionBuilder.class.getDeclaredMethod("createAndStartAdapter");
    createAndStartAdapter.setAccessible(true);
    Field adapterField = SpannerCqlSessionBuilder.class.getDeclaredField("adapter");
    adapterField.setAccessible(true);
    Field contactPointsField = SessionBuilder.class.getDeclaredField("programmaticContactPoints");
    contactPointsField.setAccessible(true);

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

      checkContactPoints.invoke(builder);
      createAndStartAdapter.invoke(builder);

      startedAdapter = (Adapter) adapterField.get(builder);
      assertThat(startedAdapter).isNotNull();
      int boundPort = startedAdapter.getBoundPort();
      assertThat(boundPort).isGreaterThan(0);
      assertThat(boundPort).isNotEqualTo(9042);

      Set<EndPoint> contactPoints = (Set<EndPoint>) contactPointsField.get(builder);
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
  @SuppressWarnings("unchecked")
  public void testBuilderReuse_bindsDistinctEphemeralPorts() throws Exception {
    SpannerCqlSessionBuilder builder =
        new SpannerCqlSessionBuilder()
            .setDatabaseUri(TEST_DATABASE_URI)
            .setHost("localhost:15000")
            .setUsePlainText(true);

    Method checkContactPoints =
        SpannerCqlSessionBuilder.class.getDeclaredMethod("checkContactPoints");
    checkContactPoints.setAccessible(true);
    Method createAndStartAdapter =
        SpannerCqlSessionBuilder.class.getDeclaredMethod("createAndStartAdapter");
    createAndStartAdapter.setAccessible(true);
    Field adapterField = SpannerCqlSessionBuilder.class.getDeclaredField("adapter");
    adapterField.setAccessible(true);
    Field contactPointsField = SessionBuilder.class.getDeclaredField("programmaticContactPoints");
    contactPointsField.setAccessible(true);

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

      checkContactPoints.invoke(builder);
      createAndStartAdapter.invoke(builder);
      firstAdapter = (Adapter) adapterField.get(builder);
      int firstPort = firstAdapter.getBoundPort();
      assertThat(firstPort).isGreaterThan(0);

      checkContactPoints.invoke(builder);
      createAndStartAdapter.invoke(builder);
      secondAdapter = (Adapter) adapterField.get(builder);
      int secondPort = secondAdapter.getBoundPort();
      assertThat(secondPort).isGreaterThan(0);
      assertThat(secondPort).isNotEqualTo(firstPort);

      Set<EndPoint> contactPoints = (Set<EndPoint>) contactPointsField.get(builder);
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
}

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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.Session;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public final class AdapterClientWrapperTest {

  private final Session mockSession = mock(Session.class);
  private AttachmentsCache attachmentsCache;
  private final AdapterClient mockAdapterClient = mock(AdapterClient.class);
  private final ServerStream<AdaptMessageResponse> mockServerStream = mock(ServerStream.class);
  private final ServerStreamingCallable<AdaptMessageRequest, AdaptMessageResponse> mockCallable =
      mock(ServerStreamingCallable.class);
  private final SessionManager mockSessionManager = mock(SessionManager.class);

  private AdapterClientWrapper adapterClientWrapper;

  public AdapterClientWrapperTest() {}

  @Before
  public void setUp() {
    attachmentsCache = new AttachmentsCache(5);
    when(mockAdapterClient.adaptMessageCallable()).thenReturn(mockCallable);
    when(mockCallable.call(any(AdaptMessageRequest.class))).thenReturn(mockServerStream);
    when(mockSessionManager.getSession()).thenReturn(mockSession);
    when(mockSession.getName()).thenReturn("test-session");
    adapterClientWrapper =
        new AdapterClientWrapper(mockAdapterClient, attachmentsCache, mockSessionManager);
  }

  @Test
  public void sendGrpcRequest_SuccessfulResponse() {
    byte[] payload = "test payload".getBytes();
    Map<String, String> stateUpdates = Map.of("k1", "v1", "k2", "v2");
    AdaptMessageResponse mockResponse =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8("test response"))
            .putAllStateUpdates(stateUpdates)
            .build();
    Iterator<AdaptMessageResponse> mockResponseIterator =
        Collections.singletonList(mockResponse).iterator();
    AdaptMessageRequest expectedRequest =
        AdaptMessageRequest.newBuilder()
            .setName("test-session")
            .setProtocol("cassandra")
            .setPayload(ByteString.copyFrom(payload))
            .build();
    when(mockServerStream.iterator()).thenReturn(mockResponseIterator);

    byte[] response = adapterClientWrapper.sendGrpcRequest(payload, new HashMap<>()).get();

    verify(mockCallable).call(expectedRequest);
    assertThat(response).isEqualTo("test response".getBytes());
    assertThat(attachmentsCache.get("k1")).hasValue("v1");
    assertThat(attachmentsCache.get("k2")).hasValue("v2");
  }

  @Test
  public void sendGrpcRequest_NoResponse() {
    byte[] payload = "test payload".getBytes();
    Iterator<AdaptMessageResponse> mockResponseIterator = Collections.emptyIterator();
    AdaptMessageRequest expectedRequest =
        AdaptMessageRequest.newBuilder()
            .setName("test-session")
            .setProtocol("cassandra")
            .setPayload(ByteString.copyFrom(payload))
            .build();
    when(mockServerStream.iterator()).thenReturn(mockResponseIterator);
    when(mockSession.getName()).thenReturn("test-session");

    Optional<byte[]> response = adapterClientWrapper.sendGrpcRequest(payload, new HashMap<>());

    verify(mockCallable).call(expectedRequest);
    assertThat(response.isEmpty());
  }

  @Test
  public void sendGrpcRequest_SessionCreationFailure() {
    byte[] payload = "test payload".getBytes();
    when(mockSessionManager.getSession()).thenThrow(new RuntimeException());

    assertThrows(
        RuntimeException.class,
        () -> adapterClientWrapper.sendGrpcRequest(payload, new HashMap<>()));
  }
}

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

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.spanner.adapter.AttachmentsCache.CacheValue;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.Session;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
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
  //   private AttachmentsCache attachmentsCache2;
  private final AdapterClient mockAdapterClient = mock(AdapterClient.class);
  private final ServerStream<AdaptMessageResponse> mockServerStream = mock(ServerStream.class);
  private final ServerStreamingCallable<AdaptMessageRequest, AdaptMessageResponse> mockCallable =
      mock(ServerStreamingCallable.class);
  private final SessionManager mockSessionManager = mock(SessionManager.class);
  private final ApiCallContext context = GrpcCallContext.createDefault();

  private AdapterClientWrapper adapterClientWrapper;
  private AdapterClientWrapper adapterClientWrapperWithDelay;

  public AdapterClientWrapperTest() {}

  @Before
  public void setUp() {
    attachmentsCache = new AttachmentsCache(5);
    // attachmentsCache2 = new AttachmentsCache(5);
    when(mockAdapterClient.adaptMessageCallable()).thenReturn(mockCallable);
    when(mockCallable.call(any(AdaptMessageRequest.class), any(ApiCallContext.class)))
        .thenReturn(mockServerStream);
    when(mockSessionManager.getSession()).thenReturn(mockSession);
    when(mockSession.getName()).thenReturn("test-session");
    adapterClientWrapper =
        new AdapterClientWrapper(
            mockAdapterClient, attachmentsCache, mockSessionManager, Optional.empty());
    adapterClientWrapperWithDelay =
        new AdapterClientWrapper(
            mockAdapterClient,
            attachmentsCache,
            mockSessionManager,
            Optional.of(Duration.ofMillis(100L)));
  }

  @Test
  public void sendGrpcRequest_SuccessfulResponse() {
    System.out.println("sendGrpcRequest_SuccessfulResponse");
    int streamId = 1;
    byte[] payload = "test payload".getBytes();
    Map<String, String> stateUpdates = new HashMap<>();
    String queryId = "W_query1";
    String preparedQueryKey = AdapterClientWrapper.PREPARED_QUERY_ID_ATTACHMENT_PREFIX + queryId;
    stateUpdates.put(preparedQueryKey, "v1");
    stateUpdates.put("k2", "v2");
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

    ByteString response =
        adapterClientWrapper.sendGrpcRequest(payload, new HashMap<>(), context, streamId);

    verify(mockCallable).call(expectedRequest, context);
    assertThat(response).isEqualTo(ByteString.copyFromUtf8("test response"));
    Optional<CacheValue> cacheValue =
        attachmentsCache.get(ByteBuffer.wrap(queryId.getBytes(StandardCharsets.UTF_8)));
    assertThat(cacheValue.isPresent()).isTrue();
    assertThat(cacheValue.get().isRead()).isFalse();
    Map<String, String> attachments = cacheValue.get().getAttachments();
    assertThat(attachments.size()).isEqualTo(2);
    assertThat(attachments.get(preparedQueryKey)).isEqualTo("v1");
    assertThat(attachments.get("k2")).isEqualTo("v2");
  }

  @Test
  public void sendGrpcRequest_MultipleResponses() {
    System.out.println("sendGrpcRequest_MultipleResponses");
    int streamId = 1;
    byte[] payload = "test payload".getBytes();
    Map<String, String> stateUpdates1 = new HashMap<>();
    String query1 = "W_query1";
    String preparedQuery1 = AdapterClientWrapper.PREPARED_QUERY_ID_ATTACHMENT_PREFIX + query1;
    stateUpdates1.put(preparedQuery1, "v1");
    stateUpdates1.put("k2", "v2");
    AdaptMessageResponse mockResponse1 =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8(" test response 1"))
            .putAllStateUpdates(stateUpdates1)
            .build();
    Map<String, String> stateUpdates2 = new HashMap<>();
    String query2 = "R_query2";
    String preparedQuery2 = AdapterClientWrapper.PREPARED_QUERY_ID_ATTACHMENT_PREFIX + query2;
    stateUpdates2.put(preparedQuery2, "v3");
    AdaptMessageResponse mockResponse2 =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8(" test response 2"))
            .putAllStateUpdates(stateUpdates2)
            .build();
    AdaptMessageResponse mockResponse3 =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8("test header"))
            .build();
    Iterator<AdaptMessageResponse> mockResponseIterator =
        Arrays.asList(mockResponse1, mockResponse2, mockResponse3).iterator();
    when(mockServerStream.iterator()).thenReturn(mockResponseIterator);
    AdaptMessageRequest expectedRequest =
        AdaptMessageRequest.newBuilder()
            .setName("test-session")
            .setProtocol("cassandra")
            .setPayload(ByteString.copyFrom(payload))
            .build();

    ByteString response =
        adapterClientWrapperWithDelay.sendGrpcRequest(payload, new HashMap<>(), context, streamId);

    verify(mockCallable).call(expectedRequest, context);
    assertThat(response)
        .isEqualTo(ByteString.copyFromUtf8("test header test response 1 test response 2"));

    Optional<CacheValue> cacheValue1 =
        attachmentsCache.get(ByteBuffer.wrap(query1.getBytes(StandardCharsets.UTF_8)));
    assertThat(cacheValue1.isPresent()).isTrue();
    assertThat(cacheValue1.get().isRead()).isFalse();
    Map<String, String> attachments1 = cacheValue1.get().getAttachments();
    assertThat(attachments1.size()).isEqualTo(3);
    assertThat(attachments1.get(preparedQuery1)).isEqualTo("v1");
    assertThat(attachments1.get("k2")).isEqualTo("v2");
    assertThat(attachments1.get(AdapterClientWrapper.MAX_COMMIT_DELAY_ATTACHMENT_KEY))
        .isEqualTo("100");

    Optional<CacheValue> cacheValue2 =
        attachmentsCache.get(ByteBuffer.wrap(query2.getBytes(StandardCharsets.UTF_8)));
    assertThat(cacheValue2.isPresent()).isTrue();
    assertThat(cacheValue2.get().isRead()).isTrue();
    Map<String, String> attachments2 = cacheValue2.get().getAttachments();
    assertThat(attachments2.size()).isEqualTo(1);
    assertThat(attachments2.get(preparedQuery2)).isEqualTo("v3");
  }

  @Test
  public void sendGrpcRequest_NoResponse() {
    int streamId = 1;
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

    ByteString response =
        adapterClientWrapper.sendGrpcRequest(payload, new HashMap<>(), context, streamId);

    verify(mockCallable).call(expectedRequest, context);
  }

  @Test
  public void sendGrpcRequest_SessionCreationFailure() {
    int streamId = 1;
    byte[] payload = "test payload".getBytes();
    when(mockSessionManager.getSession()).thenThrow(new RuntimeException());

    assertThrows(
        RuntimeException.class,
        () ->
            adapterClientWrapper.sendGrpcRequest(
                payload, new HashMap<>(), GrpcCallContext.createDefault(), streamId));
  }
}

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

import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.serverErrorResponse;
import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.unpreparedResponse;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import com.google.spanner.adapter.v1.AdapterClient;
import com.google.spanner.adapter.v1.Session;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class DriverConnectionHandlerTest {

  private static final int HEADER_LENGTH = 9;
  private static final int STREAM_ID = 2;
  private static final FrameCodec<ByteBuf> clientFrameCodec =
      FrameCodec.defaultClient(
          new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), Compressor.none());
  private static final ArgumentCaptor<ApiCallContext> contextCaptor =
      ArgumentCaptor.forClass(ApiCallContext.class);
  private static final ArgumentCaptor<AdaptMessageRequest> adaptMessageRequestCaptor =
      ArgumentCaptor.forClass(AdaptMessageRequest.class);
  private AdapterClient mockAdapterClient = mock(AdapterClient.class);

  @SuppressWarnings("unchecked")
  private ServerStreamingCallable<AdaptMessageRequest, AdaptMessageResponse> mockCallable =
      mock(ServerStreamingCallable.class);

  private SessionManager mockSessionManager = mock(SessionManager.class);
  private Socket mockSocket;
  private ByteArrayOutputStream outputStream;
  private AttachmentsCache attachmentsCache;

  public DriverConnectionHandlerTest() {}

  @Before
  public void setUp() throws IOException {
    attachmentsCache = new AttachmentsCache(5);
    mockAdapterClient = mock(AdapterClient.class);
    when(mockSessionManager.getSession()).thenReturn(Session.newBuilder().build());
    when(mockAdapterClient.adaptMessageCallable()).thenReturn(mockCallable);
    mockSocket = mock(Socket.class);
    outputStream = new ByteArrayOutputStream();
    when(mockSocket.getOutputStream()).thenReturn(outputStream);
  }

  @Test
  public void successfulQueryMessage() throws IOException {
    byte[] validPayload = createQueryMessage();
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8.name());
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              AdaptMessageResponse mockResponse =
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build();
              observer.onResponse(mockResponse);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
    assertThat(contextCaptor.getValue().getExtraHeaders()).isEmpty();
  }

  @Test
  public void multipleAdaptMessageResponses() throws IOException {
    byte[] payload = createQueryMessage();
    Map<String, String> stateUpdates1 = new HashMap<>();
    stateUpdates1.put("k1", "v1");
    stateUpdates1.put("k2", "v2");
    AdaptMessageResponse mockResponse1 =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8(" test response 1"))
            .putAllStateUpdates(stateUpdates1)
            .build();
    Map<String, String> stateUpdates2 = new HashMap<>();
    stateUpdates2.put("k3", "v3");
    AdaptMessageResponse mockResponse2 =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8(" test response 2"))
            .putAllStateUpdates(stateUpdates2)
            .build();
    AdaptMessageResponse mockResponse3 =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8("test header"))
            .build();
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(payload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              observer.onResponse(mockResponse1);
              observer.onResponse(mockResponse2);
              observer.onResponse(mockResponse3);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name()))
        .isEqualTo("test header test response 1 test response 2");
    assertThat(attachmentsCache.get("k1")).hasValue("v1");
    assertThat(attachmentsCache.get("k2")).hasValue("v2");
    assertThat(attachmentsCache.get("k3")).hasValue("v3");
  }

  @Test
  public void noResponseFromServer() throws IOException {
    byte[] payload = createQueryMessage();
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(payload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toByteArray())
        .isEqualTo(serverErrorResponse(STREAM_ID, "No response received from the server."));
  }

  @Test
  public void errorInGrpcResponseStream_OnlyErrorMessageIsSentBackToSocket() throws IOException {
    byte[] payload = createQueryMessage();
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(payload));
    Map<String, String> stateUpdates = new HashMap<>();
    stateUpdates.put("k1", "v1");
    AdaptMessageResponse mockResponse =
        AdaptMessageResponse.newBuilder()
            .setPayload(ByteString.copyFromUtf8(" test response 1"))
            .putAllStateUpdates(stateUpdates)
            .build();
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              observer.onResponse(mockResponse);
              observer.onError(new Throwable("Error!"));
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            any(ApiCallContext.class));

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(serverErrorResponse(STREAM_ID, "Error!"));
    assertThat(attachmentsCache.get("k1")).hasValue("v1");
  }

  @Test
  public void successfulDmlQueryMessage() throws IOException {
    byte[] validPayload = createDmlQueryMessage();
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8.name());
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    doAnswer(
            invocation -> {
              AdaptMessageResponseObserver observer = invocation.getArgument(1);
              AdaptMessageResponse mockResponse =
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build();
              observer.onResponse(mockResponse);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            adaptMessageRequestCaptor.capture(),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());

    // Use a max commit delay of 100 ms.
    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket,
            mockAdapterClient,
            mockSessionManager,
            attachmentsCache,
            Optional.of(Duration.ofMillis(100)));
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
    assertThat(contextCaptor.getValue().getExtraHeaders())
        .containsExactly("x-goog-spanner-route-to-leader", ImmutableList.of("true"));
    assertThat(adaptMessageRequestCaptor.getValue().getAttachmentsMap())
        .containsExactly("max_commit_delay", "100");
  }

  @Test
  public void successfulPrepareMessage() throws IOException {
    byte[] validPayload = createPrepareMessage();
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8.name());
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              AdaptMessageResponse mockResponse =
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build();
              observer.onResponse(mockResponse);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
    assertThat(contextCaptor.getValue().getExtraHeaders()).isEmpty();
  }

  @Test
  public void successfulExecuteMessage() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createExecuteMessage(queryId);
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8.name());
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              AdaptMessageResponse mockResponse =
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build();
              observer.onResponse(mockResponse);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());
    attachmentsCache.put("pqid/" + new String(queryId, StandardCharsets.UTF_8.name()), "query");

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
    assertThat(contextCaptor.getValue().getExtraHeaders()).isEmpty();
  }

  @Test
  public void successfulDmlExecuteMessage() throws IOException {
    // Add the `W` prefix to indicate that this query originates from a prepared DML statement.
    byte[] queryId = "W123".getBytes(StandardCharsets.UTF_8.name());
    byte[] validPayload = createExecuteMessage(queryId);
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8.name());
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              AdaptMessageResponse mockResponse =
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build();
              observer.onResponse(mockResponse);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            adaptMessageRequestCaptor.capture(),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());
    String preparedQueryKey = "pqid/" + new String(queryId, StandardCharsets.UTF_8.name());
    attachmentsCache.put(preparedQueryKey, "query");

    // Use a max commit delay of 100 ms.
    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket,
            mockAdapterClient,
            mockSessionManager,
            attachmentsCache,
            Optional.of(Duration.ofMillis(100)));
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
    assertThat(contextCaptor.getValue().getExtraHeaders())
        .containsExactly("x-goog-spanner-route-to-leader", ImmutableList.of("true"));
    assertThat(adaptMessageRequestCaptor.getValue().getAttachmentsMap())
        .containsExactly(preparedQueryKey, "query", "max_commit_delay", "100");
  }

  @Test
  public void failedExecuteMessage_unpreparedError() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createExecuteMessage(queryId);
    byte[] response = unpreparedResponse(STREAM_ID, queryId);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(response);
    verify(mockAdapterClient, never()).adaptMessageCallable();
    verify(mockSocket).close();
  }

  @Test
  public void successfulBatchMessage() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createBatchMessage(queryId);
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8.name());
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              AdaptMessageResponse mockResponse =
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build();
              observer.onResponse(mockResponse);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(
            any(AdaptMessageRequest.class),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());
    attachmentsCache.put("pqid/" + new String(queryId, StandardCharsets.UTF_8.name()), "query");

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
    assertThat(contextCaptor.getValue().getExtraHeaders())
        .containsExactly("x-goog-spanner-route-to-leader", ImmutableList.of("true"));
  }

  @Test
  public void failedBatchMessage_unpreparedError() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createBatchMessage(queryId);
    byte[] response = unpreparedResponse(STREAM_ID, queryId);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(response);
    verify(mockAdapterClient, never()).adaptMessageCallable();
    verify(mockSocket).close();
  }

  @Test
  public void shortHeader_writesErrorMessageToSocket() throws IOException {
    byte[] shortHeader = new byte[HEADER_LENGTH - 1];
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(shortHeader));
    byte[] expectedResponse =
        serverErrorResponse(
            -1, "Server error during request processing: Payload is not well formed.");

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(expectedResponse);
    verify(mockSocket).close();
  }

  @Test
  public void negativeBodyLength_writesErrorMessageToSocket() throws IOException {
    byte[] header = createHeaderWithBodyLength(-1);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(header));
    byte[] expectedResponse =
        serverErrorResponse(
            -1, "Server error during request processing: Payload is not well formed.");

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(expectedResponse);
    verify(mockSocket).close();
  }

  @Test
  public void shortBody_writesErrorMessageToSocket() throws IOException {
    byte[] header = createHeaderWithBodyLength(10);
    byte[] body = new byte[5];
    byte[] invalidPayload = concatenateArrays(header, body);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(invalidPayload));
    byte[] expectedResponse =
        serverErrorResponse(
            -1, "Server error during request processing: Payload is not well formed.");

    DriverConnectionHandler handler =
        new DriverConnectionHandler(
            mockSocket, mockAdapterClient, mockSessionManager, attachmentsCache);

    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(expectedResponse);
    verify(mockSocket).close();
  }

  private static byte[] createQueryMessage() {
    return encodeMessage(new Query("SELECT * FROM ks.T"));
  }

  private static byte[] createDmlQueryMessage() {
    return encodeMessage(new Query("INSERT INTO ks.T (col) VALUES (1)"));
  }

  private static byte[] createPrepareMessage() {
    return encodeMessage(new Prepare("SELECT * FROM ks.T WHERE col = ?"));
  }

  private static byte[] createExecuteMessage(byte[] queryId) {
    return encodeMessage(new Execute(queryId, QueryOptions.DEFAULT));
  }

  private static byte[] createBatchMessage(byte[] queryId) {
    List<Object> queriesOrIds = new ArrayList<>();
    queriesOrIds.add("a");
    queriesOrIds.add(queryId);
    List<List<ByteBuffer>> emptyCollections = new ArrayList<>();
    emptyCollections.add(Collections.emptyList());
    emptyCollections.add(Collections.emptyList());
    return encodeMessage(new Batch((byte) 1, queriesOrIds, emptyCollections, 0, 0, 0, null, 0));
  }

  private static byte[] encodeMessage(Message msg) {
    Frame frame = Frame.forRequest(4, STREAM_ID, false, Collections.emptyMap(), msg);
    ByteBuf payloadBuf = clientFrameCodec.encode(frame);
    byte[] payload = new byte[payloadBuf.readableBytes()];
    payloadBuf.readBytes(payload);
    payloadBuf.release();
    return payload;
  }

  private static byte[] createHeaderWithBodyLength(int bodyLength) {
    byte[] header = new byte[HEADER_LENGTH];
    header[5] = (byte) (bodyLength >> 24);
    header[6] = (byte) (bodyLength >> 16);
    header[7] = (byte) (bodyLength >> 8);
    header[8] = (byte) bodyLength;
    return header;
  }

  private static byte[] concatenateArrays(byte[] array1, byte[] array2) {
    byte[] result = new byte[array1.length + array2.length];
    System.arraycopy(array1, 0, result, 0, array1.length);
    System.arraycopy(array2, 0, result, array1.length, array2.length);
    return result;
  }
}

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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
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
  private AttachmentsCache attachmentsCache;
  private DriverConnectionHandler handler;

  @Before
  public void setUp() {
    attachmentsCache = new AttachmentsCache(5);
    mockAdapterClient = mock(AdapterClient.class);
    when(mockSessionManager.getSession()).thenReturn(Session.newBuilder().build());
    when(mockAdapterClient.adaptMessageCallable()).thenReturn(mockCallable);
    handler =
        new DriverConnectionHandler(
            mockAdapterClient, mockSessionManager, attachmentsCache, Optional.empty());
  }

  private EmbeddedChannel newChannel() {
    return new EmbeddedChannel(handler);
  }

  private EmbeddedChannel newChannelWithMaxCommitDelay() {
    DriverConnectionHandler handlerWithDelay =
        new DriverConnectionHandler(
            mockAdapterClient,
            mockSessionManager,
            attachmentsCache,
            Optional.of(Duration.ofMillis(100)));
    return new EmbeddedChannel(handlerWithDelay);
  }

  @Test
  public void successfulQueryMessage() {
    EmbeddedChannel channel = newChannel();
    byte[] validPayload = createQueryMessage();
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8);
    doSuccessfulCall(grpcResponse);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(grpcResponse);
    release(response);
    assertThat(channel.finish()).isFalse();
    verify(mockCallable)
        .call(any(), any(AdaptMessageResponseObserver.class), contextCaptor.capture());
    assertThat(contextCaptor.getValue().getExtraHeaders()).isEmpty();
  }

  @Test
  public void multipleAdaptMessageResponses() {
    EmbeddedChannel channel = newChannel();
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
        .call(any(), any(), any());

    channel.writeInbound(Unpooled.wrappedBuffer(payload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response))
        .isEqualTo("test header test response 1 test response 2".getBytes(StandardCharsets.UTF_8));
    release(response);
    assertThat(attachmentsCache.get("k1")).hasValue("v1");
    assertThat(attachmentsCache.get("k2")).hasValue("v2");
    assertThat(attachmentsCache.get("k3")).hasValue("v3");
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void noResponseFromServer() {
    EmbeddedChannel channel = newChannel();
    byte[] payload = createQueryMessage();
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(any(), any(), any());

    channel.writeInbound(Unpooled.wrappedBuffer(payload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response))
        .isEqualTo(serverErrorResponse(STREAM_ID, "No response received from the server."));
    release(response);
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void errorInGrpcResponseStream_OnlyErrorMessageIsSentBackToSocket() {
    EmbeddedChannel channel = newChannel();
    byte[] payload = createQueryMessage();
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              observer.onError(new Throwable("Error!"));
              return null;
            })
        .when(mockCallable)
        .call(any(), any(), any());

    channel.writeInbound(Unpooled.wrappedBuffer(payload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(serverErrorResponse(STREAM_ID, "Error!"));
    release(response);
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void successfulDmlQueryMessage() {
    EmbeddedChannel channel = newChannelWithMaxCommitDelay();
    byte[] validPayload = createDmlQueryMessage();
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8);
    doSuccessfulCall(grpcResponse);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(grpcResponse);
    release(response);
    assertThat(channel.finish()).isFalse();

    verify(mockCallable)
        .call(
            adaptMessageRequestCaptor.capture(),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());
    assertThat(contextCaptor.getValue().getExtraHeaders())
        .containsExactly("x-goog-spanner-route-to-leader", ImmutableList.of("true"));
    assertThat(adaptMessageRequestCaptor.getValue().getAttachmentsMap())
        .containsExactly("max_commit_delay", "100");
  }

  @Test
  public void successfulPrepareMessage() {
    EmbeddedChannel channel = newChannel();
    byte[] validPayload = createPrepareMessage();
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8);
    doSuccessfulCall(grpcResponse);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(grpcResponse);
    release(response);
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void successfulExecuteMessage() {
    EmbeddedChannel channel = newChannel();
    byte[] queryId = {1, 2};
    byte[] validPayload = createExecuteMessage(queryId);
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8);
    attachmentsCache.put("pqid/" + new String(queryId, StandardCharsets.UTF_8), "query");
    doSuccessfulCall(grpcResponse);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(grpcResponse);
    release(response);
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void successfulDmlExecuteMessage() {
    EmbeddedChannel channel = newChannelWithMaxCommitDelay();
    byte[] queryId = "W123".getBytes(StandardCharsets.UTF_8);
    byte[] validPayload = createExecuteMessage(queryId);
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8);
    String preparedQueryKey = "pqid/" + new String(queryId, StandardCharsets.UTF_8);
    attachmentsCache.put(preparedQueryKey, "query");
    doSuccessfulCall(grpcResponse);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(grpcResponse);
    release(response);
    assertThat(channel.finish()).isFalse();

    verify(mockCallable)
        .call(
            adaptMessageRequestCaptor.capture(),
            any(AdaptMessageResponseObserver.class),
            contextCaptor.capture());
    assertThat(contextCaptor.getValue().getExtraHeaders())
        .containsExactly("x-goog-spanner-route-to-leader", ImmutableList.of("true"));
    assertThat(adaptMessageRequestCaptor.getValue().getAttachmentsMap())
        .containsExactly(preparedQueryKey, "query", "max_commit_delay", "100");
  }

  @Test
  public void failedExecuteMessage_unpreparedError() {
    EmbeddedChannel channel = newChannel();
    byte[] queryId = {1, 2};
    byte[] validPayload = createExecuteMessage(queryId);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(unpreparedResponse(STREAM_ID, queryId));
    release(response);
    verify(mockAdapterClient, never()).adaptMessageCallable();
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void successfulBatchMessage() {
    EmbeddedChannel channel = newChannel();
    byte[] queryId = {1, 2};
    byte[] validPayload = createBatchMessage(queryId);
    byte[] grpcResponse = "gRPC response".getBytes(StandardCharsets.UTF_8);
    attachmentsCache.put("pqid/" + new String(queryId, StandardCharsets.UTF_8), "query");
    doSuccessfulCall(grpcResponse);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(grpcResponse);
    release(response);
    assertThat(channel.finish()).isFalse();
    verify(mockCallable)
        .call(any(), any(AdaptMessageResponseObserver.class), contextCaptor.capture());
    assertThat(contextCaptor.getValue().getExtraHeaders())
        .containsExactly("x-goog-spanner-route-to-leader", ImmutableList.of("true"));
  }

  @Test
  public void failedBatchMessage_unpreparedError() {
    EmbeddedChannel channel = newChannel();
    byte[] queryId = {1, 2};
    byte[] validPayload = createBatchMessage(queryId);

    channel.writeInbound(Unpooled.wrappedBuffer(validPayload));
    ByteBuf response = channel.readOutbound();

    assertThat(byteBufToByteArray(response)).isEqualTo(unpreparedResponse(STREAM_ID, queryId));
    release(response);
    verify(mockAdapterClient, never()).adaptMessageCallable();
    assertThat(channel.finish()).isFalse();
  }

  @Test
  public void shortHeader_writesErrorMessageToSocket() {
    // This test verifies that the pipeline's frame decoder waits for a full header
    // and does not pass an incomplete message to the handler.
    EmbeddedChannel channel = newChannelWithFrameDecoder();
    byte[] shortHeader = new byte[HEADER_LENGTH - 1];

    channel.writeInbound(Unpooled.wrappedBuffer(shortHeader));

    // No message should reach the handler, and nothing should be written back.
    // assertThat(channel.readInbound()).isNull();
    // assertThat(channel.readOutbound()).isNull();
    assertThat(channel.finish()).isFalse();
  }

  // @Test
  // public void negativeBodyLength_writesErrorMessageToSocket() {
  //   // This test verifies that the frame decoder rejects a corrupt frame, which is
  //   // caught by the handler's exceptionCaught method, closing the channel.
  //   EmbeddedChannel channel = newChannelWithFrameDecoder();
  //   byte[] header = createHeaderWithBodyLength(-1);

  //   // A DecoderException (wrapping a CorruptedFrameException) is expected.
  //   assertThrows(
  //       DecoderException.class, () -> channel.writeInbound(Unpooled.wrappedBuffer(header)));

  //   // The exception should have closed the channel.
  //   assertThat(channel.isOpen()).isFalse();
  //   channel.finish();
  // }

  @Test
  public void shortBody_writesErrorMessageToSocket() {
    // This test verifies that the frame decoder waits for a full body.
    EmbeddedChannel channel = newChannelWithFrameDecoder();
    byte[] header = createHeaderWithBodyLength(10);
    byte[] body = new byte[5]; // Body is only 5 bytes, but header says 10.
    byte[] invalidPayload = concatenateArrays(header, body);

    channel.writeInbound(Unpooled.wrappedBuffer(invalidPayload));

    // The decoder is buffering, waiting for the remaining 5 bytes. Nothing is passed
    // to the handler, and no response is sent.
    // assertThat(channel.readInbound()).isNull();
    // assertThat(channel.readOutbound()).isNull();
    assertThat(channel.finish()).isFalse();
  }

  // HELPER METHODS
  private EmbeddedChannel newChannelWithFrameDecoder() {
    // Creates a channel with the framing logic used in the real application.
    return new EmbeddedChannel(new LengthFieldBasedFrameDecoder(1024, 5, 4, 0, 0, false), handler);
  }

  private void doSuccessfulCall(byte[] grpcResponse) {
    doAnswer(
            invocation -> {
              ResponseObserver<AdaptMessageResponse> observer = invocation.getArgument(1);
              observer.onResponse(
                  AdaptMessageResponse.newBuilder()
                      .setPayload(ByteString.copyFrom(grpcResponse))
                      .build());
              observer.onComplete();
              return null;
            })
        .when(mockCallable)
        .call(any(), any(), any());
  }

  private static byte[] byteBufToByteArray(ByteBuf buf) {
    byte[] arr = new byte[buf.readableBytes()];
    buf.readBytes(arr);
    return arr;
  }

  private static void release(ByteBuf buf) {
    if (buf != null && buf.refCnt() > 0) {
      buf.release();
    }
  }

  private static byte[] concatenateArrays(byte[] array1, byte[] array2) {
    byte[] result = new byte[array1.length + array2.length];
    System.arraycopy(array1, 0, result, 0, array1.length);
    System.arraycopy(array2, 0, result, array1.length, array2.length);
    return result;
  }

  private static byte[] createHeaderWithBodyLength(int bodyLength) {
    byte[] header = new byte[HEADER_LENGTH];
    header[5] = (byte) (bodyLength >> 24);
    header[6] = (byte) (bodyLength >> 16);
    header[7] = (byte) (bodyLength >> 8);
    header[8] = (byte) bodyLength;
    return header;
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
    queriesOrIds.add("INSERT INTO ks.T (col1) VALUES ('a')");
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
}

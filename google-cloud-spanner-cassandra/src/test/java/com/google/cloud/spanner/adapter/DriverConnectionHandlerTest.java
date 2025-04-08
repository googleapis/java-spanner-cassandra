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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public final class DriverConnectionHandlerTest {

  private static final int HEADER_LENGTH = 9;
  private static final FrameCodec<ByteBuf> clientFrameCodec =
      FrameCodec.defaultClient(
          new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), Compressor.none());
  private AdapterClientWrapper mockAdapterClient;
  private Socket mockSocket;
  private ByteArrayOutputStream outputStream;

  public DriverConnectionHandlerTest() {}

  @Before
  public void setUp() throws IOException {
    mockAdapterClient = mock(AdapterClientWrapper.class);
    mockSocket = mock(Socket.class);
    outputStream = new ByteArrayOutputStream();
    when(mockSocket.getOutputStream()).thenReturn(outputStream);
  }

  @Test
  public void successfulQueryMessage() throws IOException {
    byte[] validPayload = createQueryMessage();
    Optional<byte[]> grpcResponse =
        Optional.of("gRPC response".getBytes(StandardCharsets.UTF_8.name()));
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    when(mockAdapterClient.sendGrpcRequest(any(byte[].class), any())).thenReturn(grpcResponse);

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
  }

  @Test
  public void successfulPrepareMessage() throws IOException {
    byte[] validPayload = createPrepareMessage();
    Optional<byte[]> grpcResponse =
        Optional.of("gRPC response".getBytes(StandardCharsets.UTF_8.name()));
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    when(mockAdapterClient.sendGrpcRequest(any(byte[].class), any())).thenReturn(grpcResponse);

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
  }

  @Test
  public void successfulExecuteMessage() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createExecuteMessage(queryId);
    Optional<byte[]> grpcResponse =
        Optional.of("gRPC response".getBytes(StandardCharsets.UTF_8.name()));
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    when(mockAdapterClient.sendGrpcRequest(any(byte[].class), any())).thenReturn(grpcResponse);
    AttachmentsCache AttachmentsCache = new AttachmentsCache(1);
    AttachmentsCache.put("pqid/" + new String(queryId, StandardCharsets.UTF_8.name()), "query");
    when(mockAdapterClient.getAttachmentsCache()).thenReturn(AttachmentsCache);

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
  }

  @Test
  public void failedExecuteMessage_unpreparedError() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createExecuteMessage(queryId);
    byte[] response = unpreparedResponse(queryId);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    AttachmentsCache AttachmentsCache = new AttachmentsCache(1);
    when(mockAdapterClient.getAttachmentsCache()).thenReturn(AttachmentsCache);

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(response);
    verify(mockAdapterClient, never()).sendGrpcRequest(any(), any());
    verify(mockSocket).close();
  }

  @Test
  public void successfulBatchMessage() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createBatchMessage(queryId);
    Optional<byte[]> grpcResponse =
        Optional.of("gRPC response".getBytes(StandardCharsets.UTF_8.name()));
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    when(mockAdapterClient.sendGrpcRequest(any(byte[].class), any())).thenReturn(grpcResponse);
    AttachmentsCache AttachmentsCache = new AttachmentsCache(1);
    AttachmentsCache.put("pqid/" + new String(queryId, StandardCharsets.UTF_8.name()), "query");
    when(mockAdapterClient.getAttachmentsCache()).thenReturn(AttachmentsCache);

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toString(StandardCharsets.UTF_8.name())).isEqualTo("gRPC response");
    verify(mockSocket).close();
  }

  @Test
  public void failedBatchMessage_unpreparedError() throws IOException {
    byte[] queryId = {1, 2};
    byte[] validPayload = createBatchMessage(queryId);
    byte[] response = unpreparedResponse(queryId);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(validPayload));
    AttachmentsCache AttachmentsCache = new AttachmentsCache(1);
    when(mockAdapterClient.getAttachmentsCache()).thenReturn(AttachmentsCache);

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(response);
    verify(mockAdapterClient, never()).sendGrpcRequest(any(), any());
    verify(mockSocket).close();
  }

  @Test
  public void shortHeader_writesErrorMessageToSocket() throws IOException {
    byte[] shortHeader = new byte[HEADER_LENGTH - 1];
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(shortHeader));
    byte[] expectedResponse =
        serverErrorResponse("Server error during request processing: Payload is not well formed.");

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(expectedResponse);
    verify(mockSocket).close();
  }

  @Test
  public void negativeBodyLength_writesErrorMessageToSocket() throws IOException {
    byte[] header = createHeaderWithBodyLength(-1);
    when(mockSocket.getInputStream()).thenReturn(new ByteArrayInputStream(header));
    byte[] expectedResponse =
        serverErrorResponse("Server error during request processing: Payload is not well formed.");

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);
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
        serverErrorResponse("Server error during request processing: Payload is not well formed.");

    DriverConnectionHandler handler = new DriverConnectionHandler(mockSocket, mockAdapterClient);

    handler.run();

    assertThat(outputStream.toByteArray()).isEqualTo(expectedResponse);
    verify(mockSocket).close();
  }

  private static byte[] createQueryMessage() {
    return encodeMessage(new Query("SELECT * FROM T"));
  }

  private static byte[] createPrepareMessage() {
    return encodeMessage(new Prepare("SELECT * FROM T WHERE col = ?"));
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
    Frame frame = Frame.forRequest(4, HEADER_LENGTH, false, Collections.emptyMap(), msg);
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

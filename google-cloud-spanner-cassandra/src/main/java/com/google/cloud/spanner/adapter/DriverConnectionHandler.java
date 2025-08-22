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
import static com.google.cloud.spanner.adapter.util.StringUtils.startsWith;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.cloud.spanner.adapter.metrics.BuiltInMetricsRecorder;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the connection from a driver, translating TCP data to gRPC requests and vice versa. */
final class DriverConnectionHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DriverConnectionHandler.class);
  private static final int HEADER_LENGTH = 9;
  private static final String PREPARED_QUERY_ID_ATTACHMENT_PREFIX = "pqid/";
  private static final char WRITE_ACTION_QUERY_ID_PREFIX = 'W';
  private static final String ROUTE_TO_LEADER_HEADER_KEY = "x-goog-spanner-route-to-leader";
  private static final String MAX_COMMIT_DELAY_ATTACHMENT_KEY = "max_commit_delay";
  private static final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
  private static final FrameCodec<ByteBuf> serverFrameCodec =
      FrameCodec.defaultServer(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  private final Socket socket;
  private final AdapterClientWrapper adapterClientWrapper;
  private final Optional<String> maxCommitDelayMillis;
  private static final int defaultStreamId = -1;
  private final BuiltInMetricsRecorder metricsRecorder;

  // These contexts are thread-safe and can be reused across all instances.
  private static final GrpcCallContext DEFAULT_CONTEXT = GrpcCallContext.createDefault();
  private static final Map<String, List<String>> ROUTE_TO_LEADER_HEADER_MAP =
      ImmutableMap.of(ROUTE_TO_LEADER_HEADER_KEY, Collections.singletonList("true"));
  private static final GrpcCallContext DEFAULT_CONTEXT_WITH_LAR =
      GrpcCallContext.createDefault().withExtraHeaders(ROUTE_TO_LEADER_HEADER_MAP);
  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Constructor for DriverConnectionHandler.
   *
   * @param socket The client's socket.
   * @param adapterClientWrapper The adapter client wrapper used for gRPC communication.
   * @param maxCommitDelay The max commit delay to set in requests to optimize write throughput.
   */
  public DriverConnectionHandler(
      Socket socket,
      AdapterClientWrapper adapterClientWrapper,
      BuiltInMetricsRecorder metricsRecorder,
      Optional<Duration> maxCommitDelay) {
    this.socket = socket;
    this.adapterClientWrapper = adapterClientWrapper;
    this.metricsRecorder = metricsRecorder;
    if (maxCommitDelay.isPresent()) {
      this.maxCommitDelayMillis = Optional.of(String.valueOf(maxCommitDelay.get().toMillis()));
    } else {
      this.maxCommitDelayMillis = Optional.empty();
    }
  }

  @VisibleForTesting
  public DriverConnectionHandler(
      Socket socket,
      AdapterClientWrapper adapterClientWrapper,
      BuiltInMetricsRecorder metricsRecorder) {
    this(socket, adapterClientWrapper, metricsRecorder, Optional.empty());
  }

  /** Runs the connection handler, processing incoming TCP data and sending gRPC requests. */
  @Override
  public void run() {
    LOG.debug("Handling connection from: {}", socket.getRemoteSocketAddress());

    try (BufferedInputStream inputStream = new BufferedInputStream(socket.getInputStream());
        BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream())) {
      processRequestsLoop(inputStream, outputStream);
    } catch (IOException e) {
      LOG.error(
          "Exception handling connection from {}: {}",
          socket.getRemoteSocketAddress(),
          e.getMessage(),
          e);
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Error closing socket: {}", e.getMessage());
      }
    }
  }

  private void processRequestsLoop(InputStream inputStream, OutputStream outputStream)
      throws IOException {
    // Keep processing until End-Of-Stream is reached on the input
    while (true) {
      int streamId = defaultStreamId; // Initialize with a default value.
      Instant startTime = null;
      ByteString response;
      try {
        // 1. Read and construct the message context from the input stream
        MessageContext ctx = constructMessageContext(inputStream);

        startTime = Instant.now();
        // 2. Check for EOF signaled by an empty payload
        if (ctx.payload.length == 0) {
          break; // Break out of the loop gracefully in case of EOF
        }

        // 3. Prepare the payload.
        PreparePayloadResult prepareResult = preparePayload(ctx);
        streamId = ctx.streamId;

        if (prepareResult.getAttachmentErrorResponse().isPresent()) {
          response = prepareResult.getAttachmentErrorResponse().get();
        } else {
          // 4. If attachment preparation didn't yield an immediate response, send the gRPC request.
          response =
              adapterClientWrapper.sendGrpcRequest(
                  ctx.payload,
                  prepareResult.getAttachments(),
                  prepareResult.getContext(),
                  streamId);
          // Now response holds the gRPC result, which might still be empty.
        }
      } catch (RuntimeException e) {
        // 5. Handle any error during payload construction or attachment processing.
        // Create a server error response to send back to the client.
        LOG.error("Error processing request: ", e);
        response =
            serverErrorResponse(
                streamId, "Server error during request processing: " + e.getMessage());
      }
      response.writeTo(outputStream);
      outputStream.flush();
      recordMetrics(startTime);
    }
  }

  private void recordMetrics(Instant startTime) {
    if (startTime == null) {
      return;
    }
    final long latency = Duration.between(startTime, Instant.now()).toMillis();
    metricsRecorder.recordOperationCount(1);
    metricsRecorder.recordOperationLatency(latency);
  }

  private static int readNBytesJava8(InputStream in, byte[] b, int off, int len)
      throws IOException {
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException(
          String.format("offset %d, length %d, buffer length %d", off, len, b.length));
    }

    if (len == 0) {
      return 0;
    }

    int totalBytesRead = 0;
    while (totalBytesRead < len) {
      int bytesReadInCurrentLoop = in.read(b, off + totalBytesRead, len - totalBytesRead);
      if (bytesReadInCurrentLoop == -1) {
        // EOF reached before 'len' bytes were read.
        break;
      }
      totalBytesRead += bytesReadInCurrentLoop;
    }

    return totalBytesRead;
  }

  private static class MessageContext {
    final int opCode;
    final short streamId;
    final byte[] payload;

    MessageContext(int opCode, short streamId, byte[] payload) {
      this.opCode = opCode;
      this.streamId = streamId;
      this.payload = payload;
    }

    MessageContext() {
      payload = EMPTY_BYTES;
      opCode = -1;
      streamId = -1;
    }
  }

  private MessageContext constructMessageContext(InputStream socketInputStream)
      throws IOException, IllegalArgumentException {
    byte[] header = new byte[HEADER_LENGTH];
    int bytesRead = readNBytesJava8(socketInputStream, header, 0, HEADER_LENGTH);
    if (bytesRead == 0) {
      return new MessageContext(); // EOF
    } else if (bytesRead < HEADER_LENGTH) {
      throw new IllegalArgumentException("Payload is not well formed.");
    }

    // Extract the stream id, op code and body length from the header.
    short streamId = load16BigEndian(header, 2);
    int opCode = load8Unsigned(header, 4);
    int bodyLength = load32BigEndian(header, 5);

    if (bodyLength < 0) {
      throw new IllegalArgumentException("Payload is not well formed.");
    }

    byte[] payload = new byte[HEADER_LENGTH + bodyLength];
    System.arraycopy(header, 0, payload, 0, HEADER_LENGTH);
    if (bodyLength > 0
        && readNBytesJava8(socketInputStream, payload, HEADER_LENGTH, bodyLength) < bodyLength) {
      throw new IllegalArgumentException("Payload is not well formed.");
    }

    return new MessageContext(opCode, streamId, payload);
  }

  /**
   * Reads four consecutive bytes from an array, starting at a given offset, and interprets them as
   * a single 32-bit integer in big-endian byte order.
   */
  private int load32BigEndian(byte[] bytes, int offset) {
    return ((bytes[offset] & 0xFF) << 24)
        | ((bytes[offset + 1] & 0xFF) << 16)
        | ((bytes[offset + 2] & 0xFF) << 8)
        | ((bytes[offset + 3] & 0xFF));
  }

  private short load16BigEndian(byte[] bytes, int offset) {
    return (short) (((bytes[offset] & 0xFF) << 8) | ((bytes[offset + 1] & 0xFF)));
  }

  private int load8Unsigned(byte[] bytes, int offset) {
    // Directly access the byte and apply the 0xFF mask.
    // This promotes the byte to an int and ensures the value is treated as unsigned.
    return bytes[offset] & 0xFF;
  }

  private Frame decodeFrame(byte[] payload) {
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    Frame frame = serverFrameCodec.decode(payloadBuf);
    payloadBuf.release();
    return frame;
  }

  /**
   * Attempts to prepare the given payload prior to sending the request.
   *
   * <p>This method checks the type of message encoded in the payload and sets the appropriate
   * attachment response and context. For attachments, it checks if the payload is an Execute or
   * Batch request and if it contains a queryId. If a queryId is found, it checks if a corresponding
   * prepared query exists in the global state. If a prepared query is found, it adds the prepared
   * query to the attachments map. If a prepared query is not found, it sets an error in the result
   * object.
   *
   * @param MessageContext The context contains the stream id, op code and payload to process.
   * @return A {@link PreparePayloadResult} containing the result of the operation.
   */
  private PreparePayloadResult preparePayload(MessageContext ctx) {
    switch (ctx.opCode) {
      case ProtocolConstants.Opcode.EXECUTE:
        return prepareExecuteMessage((Execute) decodeFrame(ctx.payload).message, ctx.streamId);
      case ProtocolConstants.Opcode.BATCH:
        return prepareBatchMessage((Batch) decodeFrame(ctx.payload).message, ctx.streamId);
      case ProtocolConstants.Opcode.QUERY:
        return prepareQueryMessage((Query) decodeFrame(ctx.payload).message, ctx.streamId);
      default:
        return new PreparePayloadResult(DEFAULT_CONTEXT);
    }
  }

  private PreparePayloadResult prepareExecuteMessage(Execute message, int streamId) {
    ApiCallContext context;
    Map<String, String> attachments = new HashMap<>();
    if (message.queryId != null
        && message.queryId.length > 0
        && message.queryId[0] == WRITE_ACTION_QUERY_ID_PREFIX) {
      context = DEFAULT_CONTEXT_WITH_LAR;
      maxCommitDelayMillis.ifPresent(
          delay -> attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, delay));
    } else {
      context = DEFAULT_CONTEXT;
    }
    Optional<ByteString> errorResponse =
        prepareAttachmentForQueryId(streamId, attachments, message.queryId);
    return new PreparePayloadResult(context, attachments, errorResponse);
  }

  private PreparePayloadResult prepareBatchMessage(Batch message, int streamId) {
    Optional<ByteString> attachmentErrorResponse = Optional.empty();
    Map<String, String> attachments = new HashMap<>();
    for (Object obj : message.queriesOrIds) {
      if (obj instanceof byte[]) {
        Optional<ByteString> errorResponse =
            prepareAttachmentForQueryId(streamId, attachments, (byte[]) obj);
        if (errorResponse.isPresent()) {
          attachmentErrorResponse = errorResponse;
          break;
        }
      }
    }
    maxCommitDelayMillis.ifPresent(
        delay -> attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, delay));
    // No error, return with populated attachments.
    return new PreparePayloadResult(DEFAULT_CONTEXT_WITH_LAR, attachments, attachmentErrorResponse);
  }

  private PreparePayloadResult prepareQueryMessage(Query message, int streamId) {
    ApiCallContext context;
    Map<String, String> attachments = Collections.emptyMap();
    if (startsWith(message.query, "SELECT")) {
      context = DEFAULT_CONTEXT;
    } else {
      context = DEFAULT_CONTEXT_WITH_LAR;
      if (maxCommitDelayMillis.isPresent()) {
        attachments = new HashMap<>();
        attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, maxCommitDelayMillis.get());
      }
    }
    return new PreparePayloadResult(context, attachments);
  }

  private Optional<ByteString> prepareAttachmentForQueryId(
      int streamId, Map<String, String> attachments, byte[] queryId) {
    String key = constructKey(queryId);
    Optional<String> val = adapterClientWrapper.getAttachmentsCache().get(key);
    if (!val.isPresent()) {
      return Optional.of(unpreparedResponse(streamId, queryId));
    }
    attachments.put(key, val.get());
    return Optional.empty();
  }

  private static String constructKey(byte[] queryId) {
    return PREPARED_QUERY_ID_ATTACHMENT_PREFIX + new String(queryId, StandardCharsets.UTF_8);
  }
}

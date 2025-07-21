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
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
   private final AsynchronousSocketChannel channel;
  private final AdapterClientWrapper adapterClientWrapper;
  private final Optional<String> maxCommitDelayMillis;
  private final GrpcCallContext defaultContext;
  private final GrpcCallContext defaultContextWithLAR;
  private static final Map<String, List<String>> ROUTE_TO_LEADER_HEADER_MAP =
      ImmutableMap.of(ROUTE_TO_LEADER_HEADER_KEY, Collections.singletonList("true"));
  private static final int defaultStreamId = -1;

  private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
  private ByteBuffer bodyBuffer; // Will be allocated dynamically

  /**
   * Constructor for DriverConnectionHandler.
   *
   * @param socket The client's socket.
   * @param adapterClientWrapper The adapter client wrapper used for gRPC communication.
   * @param maxCommitDelay The max commit delay to set in requests to optimize write throughput.
   */
  public DriverConnectionHandler(
      AsynchronousSocketChannel channel,
      AdapterClientWrapper adapterClientWrapper,
      Optional<Duration> maxCommitDelay) {
    this.channel = channel;
    this.adapterClientWrapper = adapterClientWrapper;
    this.defaultContext = GrpcCallContext.createDefault();
    this.defaultContextWithLAR =
        GrpcCallContext.createDefault().withExtraHeaders(ROUTE_TO_LEADER_HEADER_MAP);
    this.maxCommitDelayMillis =
        maxCommitDelay.map(duration -> String.valueOf(duration.toMillis()));
  }

  /** Kicks off the asynchronous request processing chain. */
  @Override
  public void run() {
    // Start the first read operation for the header. The rest of the process
    // will be handled by a chain of completion handlers.
    readRequestHeader();
  }

  /** Asynchronously reads the 9-byte request header. */
  private void readRequestHeader() {
    headerBuffer.clear();
    readFully(headerBuffer, new CompletionHandler<Integer, Void>() {
      @Override
      public void completed(Integer bytesRead, Void attachment) {
        // A successful read of the header triggers the body read.
        readRequestBody();
      }

      @Override
      public void failed(Throwable exc, Void attachment) {
        handleFailure("reading request header", exc);
      }
    });
  }

  /** Asynchronously reads the request body based on length from the header. */
  private void readRequestBody() {
    headerBuffer.flip();
    // In Cassandra protocol, body length is at offset 5.
    int bodyLength = ByteBuffer.wrap(headerBuffer.array(), 5, 4).getInt();
    if (bodyLength < 0) {
        handleFailure("processing request", new IOException("Invalid body length: " + bodyLength));
        return;
    }

    bodyBuffer = ByteBuffer.allocate(bodyLength);
    readFully(bodyBuffer, new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer result, Void attachment) {
            // A successful read of the body triggers request processing.
            processRequest();
          }

          @Override
          public void failed(Throwable exc, Void attachment) {
            handleFailure("reading request body", exc);
          }
        });
  }

  /** Processes the complete request and triggers the response write. */
  private void processRequest() {
    byte[] responseToWrite;
    int streamId = defaultStreamId;

    try {
      // 1. Combine header and body into a single payload array.
      byte[] payload = new byte[headerBuffer.remaining() + bodyBuffer.remaining()];
      headerBuffer.get(payload, 0, headerBuffer.remaining());
      bodyBuffer.get(payload, headerBuffer.remaining(), bodyBuffer.remaining());

      // 2. Prepare the payload (same logic as before).
      PreparePayloadResult prepareResult = preparePayload(payload);
      streamId = prepareResult.getStreamId();
      Optional<byte[]> response = prepareResult.getAttachmentErrorResponse();

      // 3. If needed, send the gRPC request (same logic as before).
      if (!response.isPresent()) {
        responseToWrite =
            adapterClientWrapper.sendGrpcRequest(
                payload, prepareResult.getAttachments(), prepareResult.getContext(), streamId);
      } else {
        responseToWrite = response.get();
      }
    } catch (RuntimeException e) {
      LOG.error("Error processing request: ", e);
      responseToWrite =
          serverErrorResponse(streamId, "Server error during request processing: " + e.getMessage());
    }

    // 4. Asynchronously write the response back to the client.
    writeResponse(ByteBuffer.wrap(responseToWrite));
  }

  /** Asynchronously writes the response back to the client. */
  private void writeResponse(ByteBuffer responseBuffer) {
    this.channel.write(responseBuffer, null, new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer result, Void attachment) {
            // After a successful write, we immediately start listening
            // for the next request header. This creates the "loop".
            if (responseBuffer.hasRemaining()) {
              // If the write was partial, continue writing the rest.
              channel.write(responseBuffer, null, this);
            } else {
              // Write is complete, start reading the next request.
              readRequestHeader();
            }
          }

          @Override
          public void failed(Throwable exc, Void attachment) {
            handleFailure("writing response", exc);
          }
        });
  }

  /**
   * Helper to ensure a ByteBuffer is completely filled from the channel.
   * `AsynchronousSocketChannel.read` does not guarantee a full read.
   */
  private void readFully(ByteBuffer buffer, CompletionHandler<Integer, Void> handler) {
    channel.read(buffer, null, new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer bytesRead, Void attachment) {
            if (bytesRead < 0) {
              // EOF: Client closed the connection cleanly.
              handleClosure();
              return;
            }
            if (buffer.hasRemaining()) {
              // If the read was partial, issue another read for the rest.
              channel.read(buffer, null, this);
            } else {
              // Buffer is full, the read is successful.
              handler.completed(buffer.position(), null);
            }
          }

          @Override
          public void failed(Throwable exc, Void attachment) {
            handler.failed(exc, null);
          }
        });
  }

  /** Centralized failure handling to log errors and close the channel. */
  private void handleFailure(String action, Throwable exc) {
    LOG.error(
        "Exception while {} for client {}: {}",
        action,
        getRemoteAddressSafe(),
        exc.getMessage(),
        exc);
    closeChannel();
  }

  /** Handles clean client disconnection (EOF). */
  private void handleClosure() {
    LOG.debug("Client {} closed the connection.", getRemoteAddressSafe());
    closeChannel();
  }

  /** Closes the channel safely. */
  private void closeChannel() {
    try {
      if (channel.isOpen()) {
        channel.close();
      }
    } catch (IOException e) {
      LOG.warn("Error closing channel: {}", e.getMessage());
    }
  }

  private String getRemoteAddressSafe() {
    try {
      return channel.getRemoteAddress().toString();
    } catch (IOException e) {
      return "[unknown]";
    }
  }


  private void processRequestsLoop(InputStream inputStream, OutputStream outputStream)
      throws IOException {
    // Keep processing until End-Of-Stream is reached on the input
    while (true) {
      byte[] responseToWrite;
      int streamId = defaultStreamId; // Initialize with a default value.
      try {
        // 1. Read and construct the payload from the input stream
        byte[] payload = constructPayload(inputStream);

        // 2. Check for EOF signaled by an empty payload
        if (payload.length == 0) {
          break; // Break out of the loop gracefully in case of EOF
        }

        // 3. Prepare the payload.
        PreparePayloadResult prepareResult = preparePayload(payload);
        streamId = prepareResult.getStreamId();
        Optional<byte[]> response = prepareResult.getAttachmentErrorResponse();

        // 4. If attachment preparation didn't yield an immediate response, send the gRPC request.
        if (!response.isPresent()) {
          responseToWrite =
              adapterClientWrapper.sendGrpcRequest(
                  payload, prepareResult.getAttachments(), prepareResult.getContext(), streamId);
          // Now response holds the gRPC result, which might still be empty.
        } else {
          responseToWrite = response.get();
        }
      } catch (RuntimeException e) {
        // 5. Handle any error during payload construction or attachment processing.
        // Create a server error response to send back to the client.
        LOG.error("Error processing request: ", e);
        responseToWrite =
            serverErrorResponse(
                streamId, "Server error during request processing: " + e.getMessage());
      }

      outputStream.write(responseToWrite);
      outputStream.flush();
    }
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
    int bytesReadInCurrentLoop;

    // Loop until the desired number of bytes are read or EOF is reached
    while (totalBytesRead < len) {
      // Calculate how many bytes are still needed
      int remaining = len - totalBytesRead;
      // Calculate the current offset in the buffer
      int currentOffset = off + totalBytesRead;

      // Attempt to read the remaining bytes
      bytesReadInCurrentLoop = in.read(b, currentOffset, remaining);

      if (bytesReadInCurrentLoop == -1) {
        // End Of Stream (EOF) reached before 'len' bytes were read.
        break;
      }

      totalBytesRead += bytesReadInCurrentLoop;
    }

    return totalBytesRead;
  }

  private byte[] constructPayload(InputStream socketInputStream)
      throws IOException, IllegalArgumentException {
    byte[] header = new byte[HEADER_LENGTH];
    int bytesRead = readNBytesJava8(socketInputStream, header, 0, HEADER_LENGTH);
    if (bytesRead == 0) {
      // EOF
      return new byte[0];
    } else if (bytesRead < HEADER_LENGTH) {
      throw new IllegalArgumentException("Payload is not well formed.");
    }

    // Extract the body length from the header.
    int bodyLength = load32BigEndian(header, 5);

    if (bodyLength < 0) {
      throw new IllegalArgumentException("Payload is not well formed.");
    }

    byte[] body = new byte[bodyLength];
    if (readNBytesJava8(socketInputStream, body, 0, bodyLength) < bodyLength) {
      throw new IllegalArgumentException("Payload is not well formed.");
    }

    // Combine the header and body into the payload.
    byte[] payload = new byte[HEADER_LENGTH + bodyLength];
    System.arraycopy(header, 0, payload, 0, HEADER_LENGTH);
    System.arraycopy(body, 0, payload, HEADER_LENGTH, bodyLength);

    return payload;
  }

  private int load32BigEndian(byte[] bytes, int offset) {
    return ByteBuffer.wrap(bytes, offset, 4).getInt();
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
   * @param payload The payload to process.
   * @return A {@link PreparePayloadResult} containing the result of the operation.
   */
  private PreparePayloadResult preparePayload(byte[] payload) {
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    Frame frame = serverFrameCodec.decode(payloadBuf);
    payloadBuf.release();

    Map<String, String> attachments = new HashMap<>();
    if (frame.message instanceof Execute) {
      return prepareExecuteMessage((Execute) frame.message, frame.streamId, attachments);
    } else if (frame.message instanceof Batch) {
      return prepareBatchMessage((Batch) frame.message, frame.streamId, attachments);
    } else if (frame.message instanceof Query) {
      return prepareQueryMessage((Query) frame.message, frame.streamId, attachments);
    } else {
      return new PreparePayloadResult(defaultContext, frame.streamId);
    }
  }

  private PreparePayloadResult prepareExecuteMessage(
      Execute message, int streamId, Map<String, String> attachments) {
    ApiCallContext context;
    if (message.queryId != null
        && message.queryId.length > 0
        && message.queryId[0] == WRITE_ACTION_QUERY_ID_PREFIX) {
      context = defaultContextWithLAR;
      if (maxCommitDelayMillis.isPresent()) {
        attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, maxCommitDelayMillis.get());
      }
    } else {
      context = defaultContext;
    }
    Optional<byte[]> errorResponse =
        prepareAttachmentForQueryId(streamId, attachments, message.queryId);
    return new PreparePayloadResult(context, streamId, attachments, errorResponse);
  }

  private PreparePayloadResult prepareBatchMessage(
      Batch message, int streamId, Map<String, String> attachments) {
    Optional<byte[]> attachmentErrorResponse = Optional.empty();
    for (Object obj : message.queriesOrIds) {
      if (obj instanceof byte[]) {
        Optional<byte[]> errorResponse =
            prepareAttachmentForQueryId(streamId, attachments, (byte[]) obj);
        if (errorResponse.isPresent()) {
          attachmentErrorResponse = errorResponse;
          break;
        }
      }
    }
    if (maxCommitDelayMillis.isPresent()) {
      attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, maxCommitDelayMillis.get());
    }
    return new PreparePayloadResult(
        defaultContextWithLAR, streamId, attachments, attachmentErrorResponse);
  }

  private PreparePayloadResult prepareQueryMessage(
      Query message, int streamId, Map<String, String> attachments) {
    ApiCallContext context;
    if (startsWith(message.query, "SELECT")) {
      context = defaultContext;
    } else {
      context = defaultContextWithLAR;
      if (maxCommitDelayMillis.isPresent()) {
        attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, maxCommitDelayMillis.get());
      }
    }
    return new PreparePayloadResult(context, streamId, attachments);
  }

  private Optional<byte[]> prepareAttachmentForQueryId(
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

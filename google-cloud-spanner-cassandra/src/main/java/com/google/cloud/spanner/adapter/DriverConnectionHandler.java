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

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the connection from a driver, translating TCP data to gRPC requests and vice versa. */
final class DriverConnectionHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DriverConnectionHandler.class);
  private static final int HEADER_LENGTH = 9;
  private static final String PREPARED_QUERY_ID_ATTACHMENT_PREFIX = "pqid/";
  private static final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
  private static final FrameCodec<ByteBuf> serverFrameCodec =
      FrameCodec.defaultServer(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  private final Socket socket;
  private final AdapterClientWrapper adapterClientWrapper;

  /**
   * Constructor for DriverConnectionHandler.
   *
   * @param socket The client's socket.
   * @param adapterClientWrapper The adapter client wrapper used for gRPC communication.
   */
  public DriverConnectionHandler(Socket socket, AdapterClientWrapper adapterClientWrapper) {
    this.socket = socket;
    this.adapterClientWrapper = adapterClientWrapper;
  }

  /** Runs the connection handler, processing incoming TCP data and sending gRPC requests. */
  @Override
  public void run() {
    LOG.info("Handling connection from: {}", socket.getRemoteSocketAddress());

    try (BufferedInputStream inputStream = new BufferedInputStream(socket.getInputStream());
        BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream())) {

      processRequest(inputStream, outputStream);

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

  private void processRequest(InputStream inputStream, OutputStream outputStream)
      throws IOException {
    // Keep processing until End-Of-Stream is reached on the input
    while (true) {
      Map<String, String> attachments = new HashMap<>();
      Optional<byte[]> responseOptional; // Using Optional to handle different response scenarios

      try {
        // 1. Read and construct the payload from the input stream
        byte[] payload = constructPayload(inputStream);

        // 2. Check for EOF signaled by an empty payload
        if (payload.length == 0) {
          break; // Break out of the loop gracefully in case of EOF
        }

        // 3. Attempt to prepare Cassandra attachments.
        // This might return:
        //    - An Optional containing a specific response (e.g., an error related to
        // attachments).
        //    - An empty Optional if processing was successful and the gRPC call should proceed.
        responseOptional = tryPrepareCassandraAttachments(attachments, payload);

        // 4. If attachment preparation didn't yield an immediate response, send the gRPC request.
        if (!responseOptional.isPresent()) {
          responseOptional = adapterClientWrapper.sendGrpcRequest(payload, attachments);
          // Now responseOptional holds the gRPC result, which might still be empty.
        }

      } catch (RuntimeException e) {
        // 5. Handle any error during payload construction or attachment processing.
        // Create a server error response to send back to the client.
        responseOptional =
            Optional.of(
                serverErrorResponse("Server error during request processing: " + e.getMessage()));
      }

      // 6. Determine the final response byte array to write.
      // If responseOptional is empty at this point, it means:
      //   a) Attachment processing completed successfully without an immediate response.
      //   b) The gRPC call was made.
      //   c) The gRPC call itself returned an empty Optional (e.g., server timeout, no specific
      // data).
      // In this case, generate a default "No response" error.
      byte[] responseToWrite =
          responseOptional.orElseGet(
              () -> {
                LOG.warn("No response received from the backend server.");
                return serverErrorResponse("No response received from the server.");
              });

      // 7. Write the determined response (success or error) to the output stream.
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
   * Attempts to prepare Cassandra attachments for the given payload.
   *
   * <p>This method checks if the payload is an Execute or Batch request and if it contains a
   * queryId. If a queryId is found, it checks if a corresponding prepared query exists in the
   * global state. If a prepared query is found, it adds the prepared query to the attachments map.
   * If a prepared query is not found, it returns an error response.
   *
   * @param attachments The map to store attachments.
   * @param payload The payload to process.
   * @return An {@link Optional} containing an error response if a prepared query is not found, or
   *     an empty {@code Optional} if the preparation was successful.
   */
  private Optional<byte[]> tryPrepareCassandraAttachments(
      Map<String, String> attachments, byte[] payload) {
    ByteBuf payloadBuf = byteBufAllocator.buffer(payload.length);
    payloadBuf.writeBytes(payload);
    Frame frame = serverFrameCodec.decode(payloadBuf);
    payloadBuf.release();

    if (frame.message instanceof Execute) {
      Execute executeMsg = (Execute) frame.message;
      return prepareAttachmentForQueryId(attachments, executeMsg.queryId);
    }
    if (frame.message instanceof Batch) {
      Batch batchMsg = (Batch) frame.message;
      for (Object obj : batchMsg.queriesOrIds) {
        if (obj instanceof byte[]) {
          Optional<byte[]> response = prepareAttachmentForQueryId(attachments, (byte[]) obj);
          if (response.isPresent()) {
            return response;
          }
        }
      }
    }

    // No error.
    return Optional.empty();
  }

  private Optional<byte[]> prepareAttachmentForQueryId(
      Map<String, String> attachments, byte[] queryId) {
    String key = constructKey(queryId);
    Optional<String> val = adapterClientWrapper.getAttachmentsCache().get(key);
    if (!val.isPresent()) {
      return Optional.of(unpreparedResponse(queryId));
    }
    attachments.put(key, val.get());
    return Optional.empty();
  }

  private static String constructKey(byte[] queryId) {
    return PREPARED_QUERY_ID_ATTACHMENT_PREFIX + new String(queryId, StandardCharsets.UTF_8);
  }
}

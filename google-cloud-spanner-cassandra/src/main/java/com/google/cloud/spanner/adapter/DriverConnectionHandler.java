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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.serverErrorResponse;
import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.unpreparedResponse;
import static com.google.cloud.spanner.adapter.util.StringUtils.startsWith;
import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

/** Handles the connection from a driver, translating TCP data to gRPC requests and vice versa. */
public final class DriverConnectionHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DriverConnectionHandler.class);
  private static final int HEADER_LENGTH = 9;
  private static final String PREPARED_QUERY_ID_ATTACHMENT_PREFIX = "pqid/";
  private static final char WRITE_ACTION_QUERY_ID_PREFIX = 'W';
  private static final String ROUTE_TO_LEADER_HEADER_KEY = "x-goog-spanner-route-to-leader";
  private static final String MAX_COMMIT_DELAY_ATTACHMENT_KEY = "max_commit_delay";
  private static final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
  private static final FrameCodec<ByteBuf> serverFrameCodec =
      FrameCodec.defaultServer(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  private static final FrameCodec<ByteBuf> clientFrameCodec =
      FrameCodec.defaultClient(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  private final Socket socket;
  private final AdapterClientWrapper adapterClientWrapper;
  private final Optional<String> maxCommitDelayMillis;
  private final GrpcCallContext defaultContext;
  private final GrpcCallContext defaultContextWithLAR;
  private final String keySpace;
  private final boolean hasHyphenInKeyspace;
  private final boolean sanitizeKeyspace;
  private static final Map<String, List<String>> ROUTE_TO_LEADER_HEADER_MAP =
      ImmutableMap.of(ROUTE_TO_LEADER_HEADER_KEY, Collections.singletonList("true"));

  /**
   * Constructor for DriverConnectionHandler.
   *
   * @param socket The client's socket.
   * @param adapterClientWrapper The adapter client wrapper used for gRPC communication.
   * @param maxCommitDelay The max commit delay to set in requests to optimize write throughput.
   * @param keySpace The corresponding keyspace name of this session(assuming it's same with
   *     database name).
   * @param sanitizeKeyspace Whether to sanitize invalid characters from keyspace
   */
  public DriverConnectionHandler(
      Socket socket,
      AdapterClientWrapper adapterClientWrapper,
      Optional<Duration> maxCommitDelay,
      String keySpace,
      boolean sanitizeKeyspace) {
    this.socket = socket;
    this.adapterClientWrapper = adapterClientWrapper;
    this.defaultContext = GrpcCallContext.createDefault();
    this.defaultContextWithLAR =
        GrpcCallContext.createDefault().withExtraHeaders(ROUTE_TO_LEADER_HEADER_MAP);
    if (maxCommitDelay.isPresent()) {
      this.maxCommitDelayMillis = Optional.of(String.valueOf(maxCommitDelay.get().toMillis()));
    } else {
      this.maxCommitDelayMillis = Optional.empty();
    }
    this.keySpace = keySpace;
    this.hasHyphenInKeyspace = keySpace.contains("-");
    this.sanitizeKeyspace = sanitizeKeyspace;
  }

  public DriverConnectionHandler(Socket socket, AdapterClientWrapper adapterClientWrapper) {
    this(socket, adapterClientWrapper, Optional.empty(), "", false);
  }

  /** Runs the connection handler, processing incoming TCP data and sending gRPC requests. */
  @Override
  public void run() {
    LOG.info("Handling connection from: {}", socket.getRemoteSocketAddress());

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
      Optional<byte[]> response;
      try {
        // 1. Read and construct the payload from the input stream
        byte[] payload = constructPayload(inputStream);

        // 2. Check for EOF signaled by an empty payload
        if (payload.length == 0) {
          break; // Break out of the loop gracefully in case of EOF
        }

        // 3. Prepare the payload.
        PreparePayloadResult prepareResult = preparePayload(payload);
        // If there exists a sanitized payload after removing hyphens in keyspace, replace current
        // payload.
        if (prepareResult.sanitizedPayload != null) {
          payload = prepareResult.sanitizedPayload;
        }
        response = prepareResult.getAttachmentErrorResponse();

        // 4. If attachment preparation didn't yield an immediate response, send the gRPC request.
        if (!response.isPresent()) {
          response =
              adapterClientWrapper.sendGrpcRequest(
                  payload, prepareResult.getAttachments(), prepareResult.getContext());
          // Now response holds the gRPC result, which might still be empty.
        }

      } catch (RuntimeException e) {
        // 5. Handle any error during payload construction or attachment processing.
        // Create a server error response to send back to the client.
        LOG.error("Error processing request: ", e);
        response =
            Optional.of(
                serverErrorResponse("Server error during request processing: " + e.getMessage()));
      }

      // 6. Determine the final response byte array to write.
      // If response is empty at this point, it means:
      //   a) Attachment processing completed successfully without an immediate response.
      //   b) The gRPC call was made.
      //   c) The gRPC call itself returned an empty Optional (e.g., server timeout, no specific
      // data).
      // In this case, generate a default "No response" error.
      byte[] responseToWrite =
          response.orElseGet(
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
      return prepareExecuteMessage((Execute) frame.message, attachments);
    } else if (frame.message instanceof Batch) {
      return prepareBatchMessage((Batch) frame.message, attachments);
    } else if (frame.message instanceof Query) {
      return prepareQueryMessage(frame, attachments);
    } else {
      return new PreparePayloadResult(defaultContext);
    }
  }

  private PreparePayloadResult prepareExecuteMessage(
      Execute message, Map<String, String> attachments) {
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
    Optional<byte[]> errorResponse = prepareAttachmentForQueryId(attachments, message.queryId);
    return new PreparePayloadResult(context, attachments, errorResponse);
  }

  private PreparePayloadResult prepareBatchMessage(Batch message, Map<String, String> attachments) {
    Optional<byte[]> attachmentErrorResponse = Optional.empty();
    for (Object obj : message.queriesOrIds) {
      if (obj instanceof byte[]) {
        Optional<byte[]> errorResponse = prepareAttachmentForQueryId(attachments, (byte[]) obj);
        if (errorResponse.isPresent()) {
          attachmentErrorResponse = errorResponse;
          break;
        }
      }
    }
    if (maxCommitDelayMillis.isPresent()) {
      attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, maxCommitDelayMillis.get());
    }
    return new PreparePayloadResult(defaultContextWithLAR, attachments, attachmentErrorResponse);
  }

  private PreparePayloadResult prepareQueryMessage(Frame frame, Map<String, String> attachments) {
    ApiCallContext context;
    boolean hasSanitizedPayload = false;
    Query message = (Query) frame.message;
    if (hasHyphenInKeyspace && sanitizeKeyspace && message.query.contains(keySpace)) {
      try {
        // Query.query is a final string so we can't directly alter it.
        Field stringField = Query.class.getDeclaredField("query");
        stringField.setAccessible(true);
        stringField.set(message, sanitizeHyphenFromKeyspace(message.query, keySpace));
        hasSanitizedPayload = true;
      } catch (IllegalAccessException | IllegalArgumentException | NoSuchFieldException ex) {
        throw new RuntimeException(ex);
      }
    }
    if (startsWith(message.query, "SELECT")) {
      context = defaultContext;
    } else {
      context = defaultContextWithLAR;
      if (maxCommitDelayMillis.isPresent()) {
        attachments.put(MAX_COMMIT_DELAY_ATTACHMENT_KEY, maxCommitDelayMillis.get());
      }
    }
    PreparePayloadResult res = new PreparePayloadResult(context, attachments);
    if (hasSanitizedPayload) {
      ByteBuf sanitizedPayloadBuf = clientFrameCodec.encode(frame);
      int size = sanitizedPayloadBuf.readableBytes();
      byte[] sanitizedPayloadArr = new byte[size];
      sanitizedPayloadBuf.getBytes(sanitizedPayloadBuf.readerIndex(), sanitizedPayloadArr);
      sanitizedPayloadBuf.release();
      res.setSanitizedPayload(sanitizedPayloadArr);
    }
    return res;
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

  /**
   * Sanitizes a CQL query by removing hyphens from a known keyspace name.
   *
   * <p>It is case-insensitive in its matching of the keyspace name.
   *
   * @param cqlQuery The raw CQL query string.
   * @param keyspaceWithHyphens The exact keyspace name that contains hyphens, e.g.,
   *     "my-app-keyspace".
   * @return A sanitized CQL query string.
   */
  public static String sanitizeHyphenFromKeyspace(String cqlQuery, String keyspaceWithHyphens) {
    // --- Guard Clauses ---
    if (cqlQuery == null || cqlQuery.isEmpty()) {
      return cqlQuery;
    }
    if (keyspaceWithHyphens == null
        || keyspaceWithHyphens.isEmpty()
        || !keyspaceWithHyphens.contains("-")) {
      return cqlQuery;
    }

    final String sanitizedKeyspaceContent = keyspaceWithHyphens.replace("-", "_");

    // 1. ('(?:''|[^'])*')     : Group 1: A single-quoted string literal to be ignored.
    // 2. (\"<keyspace>\")        : Group 2: A double-quoted identifier to be sanitized.
    // 3. (\b<keyspace>(?=...)) : Group 3: An unquoted identifier to be sanitized.
    final Pattern pattern =
        Pattern.compile(
            "('(?:''|[^'])*')|(\""
                + Pattern.quote(keyspaceWithHyphens)
                + "\")|(\\b"
                + Pattern.quote(keyspaceWithHyphens)
                + "(?=[\\s.;]|$))",
            Pattern.CASE_INSENSITIVE);

    Matcher matcher = pattern.matcher(cqlQuery);
    StringBuffer sb = new StringBuffer();

    while (matcher.find()) {
      if (matcher.group(2) != null) {
        // Case 2: Matched a double-quoted keyspace, e.g., "my-app-keyspace"
        // We must replace it with the sanitized content wrapped in quotes.
        String replacement = "\"" + sanitizedKeyspaceContent + "\"";
        matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));

      } else if (matcher.group(3) != null) {
        // Case 3: Matched an unquoted keyspace, e.g., my-app-keyspace
        // We replace it with just the sanitized content.
        matcher.appendReplacement(sb, Matcher.quoteReplacement(sanitizedKeyspaceContent));

      } else {
        // Case 1 (or no match for group 2/3): Matched a single-quoted literal.
        // Append it unchanged.
        matcher.appendReplacement(sb, matcher.group(0));
      }
    }
    matcher.appendTail(sb);

    return sb.toString();
  }
}

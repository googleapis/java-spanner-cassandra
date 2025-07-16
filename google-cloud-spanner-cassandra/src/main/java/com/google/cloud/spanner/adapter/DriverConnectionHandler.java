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
import java.io.IOException;
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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DriverConnectionHandler {
  private final AsynchronousSocketChannel channel;
  private final AdapterClientWrapper adapterClientWrapper;
  private static final Logger LOG = LoggerFactory.getLogger(DriverConnectionHandler.class);
  private static final int HEADER_LENGTH = 9;
  private static final String PREPARED_QUERY_ID_ATTACHMENT_PREFIX = "pqid/";
  private static final char WRITE_ACTION_QUERY_ID_PREFIX = 'W';
  private static final String ROUTE_TO_LEADER_HEADER_KEY = "x-goog-spanner-route-to-leader";
  private static final String MAX_COMMIT_DELAY_ATTACHMENT_KEY = "max_commit_delay";
  private static final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
  private static final FrameCodec<ByteBuf> serverFrameCodec =
      FrameCodec.defaultServer(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  private final Optional<String> maxCommitDelayMillis;
  private final GrpcCallContext defaultContext;
  private final GrpcCallContext defaultContextWithLAR;
  private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
  private static final Map<String, List<String>> ROUTE_TO_LEADER_HEADER_MAP =
      ImmutableMap.of(ROUTE_TO_LEADER_HEADER_KEY, Collections.singletonList("true"));
  private static final int defaultStreamId = -1;

  // NEW: Queue and flag to serialize write operations and prevent WritePendingException.
  private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean isWriting = new AtomicBoolean(false);
  private final WriteHandler writeHandler = new WriteHandler();

  /** NEW: CompletionHandler for serializing writes to the channel. */
  private class WriteHandler implements CompletionHandler<Integer, Void> {
    @Override
    public void completed(Integer bytesWritten, Void attachment) {
      if (!writeQueue.isEmpty()) {
        // There's more data in the queue, write the next message.
        ByteBuffer nextResponse = writeQueue.poll();
        channel.write(nextResponse, null, this);
      } else {
        // The queue is empty, release the write lock.
        isWriting.set(false);
        // Check again in case a response was added while we were releasing the lock.
        if (!writeQueue.isEmpty()) {
          tryProcessWriteQueue();
        }
      }
    }

    @Override
    public void failed(Throwable t, Void attachment) {
      isWriting.set(false); // Release the lock on failure.
      LOG.error("Error writing to channel", t);
    }
  }

  public DriverConnectionHandler(
      AsynchronousSocketChannel channel,
      AdapterClientWrapper adapterClientWrapper,
      Optional<Duration> maxCommitDelay) {
    this.channel = channel;
    this.adapterClientWrapper = adapterClientWrapper;
    this.defaultContext = GrpcCallContext.createDefault();
    this.defaultContextWithLAR =
        GrpcCallContext.createDefault().withExtraHeaders(ROUTE_TO_LEADER_HEADER_MAP);
    if (maxCommitDelay.isPresent()) {
      this.maxCommitDelayMillis = Optional.of(String.valueOf(maxCommitDelay.get().toMillis()));
    } else {
      this.maxCommitDelayMillis = Optional.empty();
    }
  }

  /**
   * NEW: Safely queues a response to be written and starts the write process if it's not already
   * running. This is the only method that should be used to write to the channel.
   */
  public void queueResponseAndTryToWrite(ByteBuffer response) {
    writeQueue.offer(response);
    tryProcessWriteQueue();
  }

  /**
   * NEW: Checks if a write is in progress. If not, it starts writing the next message from the
   * queue.
   */
  private void tryProcessWriteQueue() {
    // Atomically check if a write is in progress and, if not, claim the "write lock".
    if (isWriting.compareAndSet(false, true)) {
      if (!writeQueue.isEmpty()) {
        ByteBuffer response = writeQueue.poll();
        channel.write(response, null, writeHandler);
      } else {
        // Nothing to write, release the lock immediately.
        isWriting.set(false);
      }
    }
  }

  public void startReading() {
    // Initiate an async read for the header. The CompletionHandler will be called when it's done.
    channel.read(
        headerBuffer,
        null,
        new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer bytesRead, Void attachment) {
            if (bytesRead < 0) { // End of stream
              closeChannel();
              return;
            }

            // We have the header, now read the body
            headerBuffer.flip();
            int bodyLength = headerBuffer.getInt(5);
            ByteBuffer bodyBuffer = ByteBuffer.allocate(bodyLength);

            readBody(bodyBuffer);
          }

          @Override
          public void failed(Throwable t, Void attachment) {
            // MODIFIED: Use the write queue for all writes.
            queueResponseAndTryToWrite(serverErrorResponse(defaultStreamId, t.getMessage()));
            LOG.error("Error reading from channel", t);
          }
        });
  }

  private void readBody(ByteBuffer bodyBuffer) {
    channel.read(
        bodyBuffer,
        null,
        new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer bytesRead, Void attachment) {
            if (bodyBuffer.hasRemaining()) {
              // The body hasn't been fully read yet, issue another read
              channel.read(bodyBuffer, null, this);
              return;
            }

            // Body is fully read, process the complete message
            bodyBuffer.flip();
            processCompletePayload(
                (ByteBuffer)
                    ByteBuffer.allocate(headerBuffer.capacity() + bodyBuffer.capacity())
                        .put(headerBuffer)
                        .put(bodyBuffer)
                        .flip());

            // Reset and start reading the next message header
            headerBuffer.clear();
            startReading();
          }

          @Override
          public void failed(Throwable t, Void attachment) {
            LOG.error("Error reading message body", t);
            // MODIFIED: Use the write queue for all writes.
            queueResponseAndTryToWrite(serverErrorResponse(defaultStreamId, t.getMessage()));
          }
        });
  }

  private void processCompletePayload(ByteBuffer payload) {
    PreparePayloadResult prepareResult = preparePayload(payload);
    int streamId = prepareResult.getStreamId();
    Optional<ByteBuffer> response = prepareResult.getAttachmentErrorResponse();

    // If attachment preparation didn't yield an immediate response, send the gRPC request.
    if (!response.isPresent()) {
      // MODIFIED: Pass `this` handler instead of the raw channel so the wrapper can call back.
      adapterClientWrapper.sendGrpcRequest(
          payload, prepareResult.getAttachments(), prepareResult.getContext(), streamId, this);
    } else {
      LOG.error("Attachment error");
      // MODIFIED: Use the write queue for all writes.
      queueResponseAndTryToWrite(response.get());
    }
  }

  private PreparePayloadResult preparePayload(ByteBuffer payload) {
    // This method can be optimized by using Unpooled.wrappedBuffer(header, body)
    // instead of creating a new combined buffer, but that requires changing the
    // call structure slightly. For now, we leave it as is to focus on the write fix.
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
    Optional<ByteBuffer> errorResponse =
        prepareAttachmentForQueryId(streamId, attachments, message.queryId);
    return new PreparePayloadResult(context, streamId, attachments, errorResponse);
  }

  private PreparePayloadResult prepareBatchMessage(
      Batch message, int streamId, Map<String, String> attachments) {
    Optional<ByteBuffer> attachmentErrorResponse = Optional.empty();
    for (Object obj : message.queriesOrIds) {
      if (obj instanceof byte[]) {
        Optional<ByteBuffer> errorResponse =
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

  private Optional<ByteBuffer> prepareAttachmentForQueryId(
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

  private void closeChannel() {
    try {
      if (channel.isOpen()) {
        channel.close();
      }
    } catch (IOException e) {
      LOG.error("Error closing channel: ", e);
    }
  }
}

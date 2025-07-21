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
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdapterClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles connection logic for a single driver connection on a Netty channel. This class is
 * stateful for each connection but does not manage its own threads.
 */
final class DriverConnectionHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(DriverConnectionHandler.class);
  private static final String PREPARED_QUERY_ID_ATTACHMENT_PREFIX = "pqid/";
  private static final char WRITE_ACTION_QUERY_ID_PREFIX = 'W';
  private static final String ROUTE_TO_LEADER_HEADER_KEY = "x-goog-spanner-route-to-leader";
  private static final String MAX_COMMIT_DELAY_ATTACHMENT_KEY = "max_commit_delay";
  private static final String PROTOCOL_CASSANDRA = "cassandra";
  private static final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
  private static final FrameCodec<ByteBuf> serverFrameCodec =
      FrameCodec.defaultServer(new ByteBufPrimitiveCodec(byteBufAllocator), Compressor.none());
  private final AdapterClient adapterClient;
  private final SessionManager sessionManager;
  private final AttachmentsCache attachmentsCache;
  private final Optional<String> maxCommitDelayMillis;
  private final GrpcCallContext defaultContext;
  private final GrpcCallContext defaultContextWithLAR;
  private static final Map<String, List<String>> ROUTE_TO_LEADER_HEADER_MAP =
      ImmutableMap.of(ROUTE_TO_LEADER_HEADER_KEY, Collections.singletonList("true"));
  private static final int defaultStreamId = -1;

  DriverConnectionHandler(
      AdapterClient adapterClient,
      SessionManager sessionManager,
      AttachmentsCache attachmentsCache,
      Optional<Duration> maxCommitDelay) {
    this.adapterClient = adapterClient;
    this.sessionManager = sessionManager;
    this.attachmentsCache = attachmentsCache;
    this.defaultContext = GrpcCallContext.createDefault();
    this.defaultContextWithLAR =
        GrpcCallContext.createDefault().withExtraHeaders(ROUTE_TO_LEADER_HEADER_MAP);
    if (maxCommitDelay.isPresent()) {
      this.maxCommitDelayMillis = Optional.of(String.valueOf(maxCommitDelay.get().toMillis()));
    } else {
      this.maxCommitDelayMillis = Optional.empty();
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("Accepted client connection from: {}", ctx.channel().remoteAddress());
    super.channelActive(ctx);
  }

  /** This method is called by the Netty event loop whenever a complete data frame is received. */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf frame = (ByteBuf) msg;
    try {
      processRequest(ctx, frame);
    } finally {
      // The LengthFieldBasedFrameDecoder returns a retained slice of the buffer,
      // so we must release it after processing.
      frame.release();
    }
  }

  private void processRequest(ChannelHandlerContext ctx, ByteBuf frame) {
    int streamId = defaultStreamId;
    try {
      // The frame decoder ensures we have a full payload, so we can read its bytes.
      byte[] payload = new byte[frame.readableBytes()];
      frame.readBytes(payload);

      PreparePayloadResult prepareResult = preparePayload(payload);
      Optional<byte[]> response = prepareResult.getAttachmentErrorResponse();

      if (!response.isPresent()) {
        adaptMessageAsync(ctx, prepareResult.getStreamId(), payload, prepareResult);
        return;
      }
      // Write the immediate error response back to the client.
      ctx.writeAndFlush(Unpooled.wrappedBuffer(response.get()));

    } catch (RuntimeException e) {
      LOG.error("Error processing request: ", e);
      byte[] errorResponse =
          serverErrorResponse(
              streamId, "Server error during request processing: " + e.getMessage());
      ctx.writeAndFlush(Unpooled.wrappedBuffer(errorResponse));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught on connection, closing channel.", cause);
    ctx.close();
  }

  private PreparePayloadResult preparePayload(byte[] payload) {
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    Frame frame = serverFrameCodec.decode(payloadBuf);
    payloadBuf.release(); // decode() creates a new message, so we can release this buffer.

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

  private void adaptMessageAsync(
      ChannelHandlerContext ctx, int streamId, byte[] payload, PreparePayloadResult prepareResult) {
    AdaptMessageRequest request =
        AdaptMessageRequest.newBuilder()
            .setName(sessionManager.getSession().getName())
            .setProtocol(PROTOCOL_CASSANDRA)
            .putAllAttachments(prepareResult.getAttachments())
            .setPayload(ByteString.copyFrom(payload))
            .build();
    // The observer now takes the ChannelHandlerContext to write the response back.
    AdaptMessageResponseObserver responseObserver =
        new AdaptMessageResponseObserver(streamId, ctx, attachmentsCache);
    adapterClient
        .adaptMessageCallable()
        .call(request, responseObserver, prepareResult.getContext());
  }

  // The prepare...Message and other helper methods remain largely the same.
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
    Optional<String> val = attachmentsCache.get(key);
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

/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.google.cloud.spanner.adapter;

import static com.google.cloud.spanner.adapter.util.ErrorMessageUtils.serverErrorResponse;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ResponseObserver} that handles streaming responses from the {@code AdaptMessage} gRPC
 * call and writes the assembled payload back to the Netty channel.
 *
 * <p>It is stateful for the duration of one gRPC call, buffering all incoming {@link
 * AdaptMessageResponse} payloads in memory. During the response assembly, the <strong>last</strong>
 * payload received from the stream contains the message header and must be written first.
 */
class AdaptMessageResponseObserver implements ResponseObserver<AdaptMessageResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(AdaptMessageResponseObserver.class);
  private final int streamId;
  private final AttachmentsCache attachmentsCache;
  private final ChannelHandlerContext ctx;

  private final List<ByteString> collectedPayloads = new ArrayList<>();

  AdaptMessageResponseObserver(
      int streamId, ChannelHandlerContext ctx, AttachmentsCache attachmentsCache) {
    this.streamId = streamId;
    this.attachmentsCache = attachmentsCache;
    this.ctx = ctx;
  }

  @Override
  public void onResponse(AdaptMessageResponse response) {
    // Process each response message as it arrives from the stream.
    response.getStateUpdatesMap().forEach(attachmentsCache::put);
    collectedPayloads.add(response.getPayload());
  }

  @Override
  public void onError(Throwable t) {
    // If the gRPC call fails, write an error frame back to the client channel.
    LOG.error("Error executing AdaptMessage request: ", t);
    byte[] errorPayload = serverErrorResponse(streamId, t.getMessage());
    ctx.writeAndFlush(Unpooled.wrappedBuffer(errorPayload));
  }

  @Override
  public void onComplete() {
    // When the stream is finished, process the collected payloads.
    if (collectedPayloads.isEmpty()) {
      byte[] errorPayload = serverErrorResponse(streamId, "No response received from the server.");
      ctx.writeAndFlush(Unpooled.wrappedBuffer(errorPayload));
      return;
    }

    // Use a CompositeByteBuf for zero-copy stitching of the payloads.
    final int numPayloads = collectedPayloads.size();
    CompositeByteBuf composite = ctx.alloc().compositeBuffer(numPayloads);
    try {
      // In case of multiple responses, the last response contains the header.
      // So add it to our composite buffer first.
      composite.addComponent(
          true,
          Unpooled.wrappedBuffer(collectedPayloads.get(numPayloads - 1).asReadOnlyByteBuffer()));

      // Then add the remaining responses.
      for (int i = 0; i < numPayloads - 1; i++) {
        composite.addComponent(
            true, Unpooled.wrappedBuffer(collectedPayloads.get(i).asReadOnlyByteBuffer()));
      }
      ctx.writeAndFlush(composite);
    } catch (Exception e) {
      // Release buffer in case of error.
      composite.release();
      LOG.error("Error while stitching payloads: ", e);
      onError(e);
    }
  }

  @Override
  public void onStart(StreamController controller) {
    // Do nothing. Flow control could be implemented here if needed.
  }
}

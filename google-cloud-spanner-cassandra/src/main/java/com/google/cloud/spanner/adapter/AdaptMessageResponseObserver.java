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

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ResponseObserver} that handles streaming responses from the {@code AdaptMessage} gRPC
 * call, updates the states and offers the payloads to the write queue.
 *
 * <p>It is stateful for the duration of one gRPC call, buffering all incoming {@link
 * AdaptMessageResponse} payloads in memory. During the response assembly, the <strong>last</strong>
 * payload received from the stream contains the message header and must be written first.
 */
class AdaptMessageResponseObserver implements ResponseObserver<AdaptMessageResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(AdaptMessageResponseObserver.class);
  private final int streamId;
  private final AttachmentsCache attachmentsCache;
  private final BlockingQueue<ByteString> writeQueue;

  private List<ByteString> collectedPayloads = new ArrayList<>();

  AdaptMessageResponseObserver(
      int streamId, BlockingQueue<ByteString> writeQueue, AttachmentsCache attachmentsCache) {
    this.streamId = streamId;
    this.attachmentsCache = attachmentsCache;
    this.writeQueue = writeQueue;
  }

  @Override
  public void onResponse(AdaptMessageResponse response) {
    // Process each response message as it arrives from the stream.
    response.getStateUpdatesMap().forEach(attachmentsCache::put);
    collectedPayloads.add(response.getPayload());
  }

  @Override
  public void onError(Throwable t) {
    // If the gRPC call fails, write failure to the socket.
    LOG.error("Error executing AdaptMessage request: ", t);
    writeQueue.offer(ByteString.copyFrom(serverErrorResponse(streamId, t.getMessage())));
  }

  @Override
  public void onComplete() {
    // When the stream is finished, process the collected payloads.
    if (collectedPayloads.isEmpty()) {
      writeQueue.offer(
          ByteString.copyFrom(
              serverErrorResponse(streamId, "No response received from the server.")));
      return;
    }

    final int numPayloads = collectedPayloads.size();
    writeQueue.offer(collectedPayloads.get(numPayloads - 1));
    for (int i = 0; i < numPayloads - 1; i++) {
      writeQueue.offer(collectedPayloads.get(i));
    }
    collectedPayloads.clear();
  }

  @Override
  public void onStart(StreamController controller) {
    // Do nothing.
  }
}

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
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ResponseObserver} that handles streaming responses from the {@code AdaptMessage} gRPC
 * call.
 */
class AdaptMessageResponseObserver implements ResponseObserver<AdaptMessageResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(AdaptMessageResponseObserver.class);
  private final int streamId;
  private final OutputStream outputStream;
  private final AttachmentsCache attachmentsCache;
  private List<ByteString> collectedPayloads = new ArrayList<>();

  AdaptMessageResponseObserver(
      int streamId, OutputStream outputStream, AttachmentsCache attachmentsCache) {
    this.streamId = streamId;
    this.outputStream = outputStream;
    this.attachmentsCache = attachmentsCache;
  }

  @Override
  public void onResponse(AdaptMessageResponse response) {
    // Process each response message as it arrives from the stream.
    response.getStateUpdatesMap().forEach(attachmentsCache::put);
    collectedPayloads.add(response.getPayload());
  }

  @Override
  public void onError(Throwable t) {
    // If the gRPC call fails, write
    LOG.error("Error executing AdaptMessage request: ", t);
    try {
      outputStream.write(serverErrorResponse(streamId, t.getMessage()));
      outputStream.flush();
    } catch (IOException e) {
      LOG.error("Error writing to socket: ", e);
    }
  }

  @Override
  public void onComplete() {
    // When the stream is finished, process the collected payloads.
    if (collectedPayloads.isEmpty()) {
      try {
        outputStream.write(serverErrorResponse(streamId, "No response received from the server."));
        outputStream.flush();
      } catch (IOException e) {
        LOG.error("Error writing to socket: ", e);
      }
      return;
    }

    final int numPayloads = collectedPayloads.size();
    // In case of multiple responses, the last response contains the header.
    // So write it first.
    try {
      outputStream.write(collectedPayloads.get(numPayloads - 1).toByteArray());
      // Then write the remaining responses.
      for (int i = 0; i < numPayloads - 1; i++) {
        outputStream.write(collectedPayloads.get(i).toByteArray());
      }
      outputStream.flush();
    } catch (IOException e) {
      LOG.error("Error writing to socket: ", e);
    }
  }

  @Override
  public void onStart(StreamController controller) {
    // Do nothing.
  }
}

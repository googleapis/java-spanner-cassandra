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
 * call and writes them back to the socket.
 *
 * <p>It is stateful for the duration of one gRPC call, buffering all incoming {@link
 * AdaptMessageResponse} payloads in memory. During the response assembly, the <strong>last</strong>
 * payload received from the stream contains the message header and must be written first.
 *
 * <p>This class is not responsible for resource management. The lifecycle of the {@link
 * OutputStream} and the {@link AttachmentsCache} must be managed outside. This class will NOT close
 * the output stream upon successful completion or error.
 */
class AdaptMessageResponseObserver implements ResponseObserver<AdaptMessageResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(AdaptMessageResponseObserver.class);
  private final int streamId;
  private final OutputStream outputStream;
  private final AttachmentsCache attachmentsCache;
  private final DriverConnectionHandler driverConnectionHandler;

  private List<ByteString> collectedPayloads = new ArrayList<>();

  AdaptMessageResponseObserver(
      int streamId,
      OutputStream outputStream,
      AttachmentsCache attachmentsCache,
      DriverConnectionHandler driverConnectionHandler) {
    this.streamId = streamId;
    this.outputStream = outputStream;
    this.attachmentsCache = attachmentsCache;
    this.driverConnectionHandler = driverConnectionHandler;
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
      synchronized (outputStream) {
        outputStream.write(serverErrorResponse(streamId, t.getMessage()));
        outputStream.flush();
      }
    } catch (IOException e) {
      LOG.error("Error writing to socket for streamId: {}", streamId, e);
    }
  }

  @Override
  public void onComplete() {
    // When the stream is finished, process the collected payloads.
    try {
      if (collectedPayloads.isEmpty()) {
        synchronized (outputStream) {
          outputStream.write(
              serverErrorResponse(streamId, "No response received from the server."));
          outputStream.flush();
        }
        return;
      }

      final int numPayloads = collectedPayloads.size();
      synchronized (outputStream) {
        // In case of multiple responses, the last response contains the header.
        // So write it first.
        outputStream.write(collectedPayloads.get(numPayloads - 1).toByteArray());
        // Then write the remaining responses.
        for (int i = 0; i < numPayloads - 1; i++) {
          outputStream.write(collectedPayloads.get(i).toByteArray());
        }
        outputStream.flush();
      }
    } catch (IOException e) {
      LOG.error("Error writing to socket for streamId: {}", streamId, e);
    } 
  }

  @Override
  public void onStart(StreamController controller) {
    // Do nothing.
  }
}

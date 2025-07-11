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

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ServerStream;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import com.google.spanner.adapter.v1.AdapterClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wraps an {@link AdapterClient} to manage gRPC communication with the Adapter service. */
final class AdapterClientWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterClientWrapper.class);
  private final AdapterClient adapterClient;
  private final AttachmentsCache attachmentsCache;
  private final SessionManager sessionManager;

  /**
   * Constructs a wrapper around the AdapterClient responsible for procession gRPC communication.
   *
   * @param adapterClient Stub used to communicate with the Adapter service.
   * @param attachmentsCache The global cache for the attachments.
   * @param sessionManager The manager providing session for requests.
   */
  AdapterClientWrapper(
      AdapterClient adapterClient,
      AttachmentsCache attachmentsCache,
      SessionManager sessionManager) {
    this.adapterClient = adapterClient;
    this.attachmentsCache = attachmentsCache;
    this.sessionManager = sessionManager;
  }

  /**
   * Sends a gRPC request to the adapter to process a message.
   *
   * @param payload The byte array payload of the message to send.
   * @param attachments A map of string key-value pairs to be included as attachments in the
   *     request.
   * @param streamId The stream id of the message to send.
   * @return A byte array payload of the adapter's response.
   */
  byte[] sendGrpcRequest(
      byte[] payload, Map<String, String> attachments, ApiCallContext context, int streamId) {
    // 1. Build the gRPC request object.
    AdaptMessageRequest request =
        AdaptMessageRequest.newBuilder()
            .setName(sessionManager.getSession().getName())
            .setProtocol("cassandra")
            .putAllAttachments(attachments)
            .setPayload(ByteString.copyFrom(payload))
            .build();

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      ServerStream<AdaptMessageResponse> serverStream =
          adapterClient.adaptMessageCallable().call(request, context);

      ByteString lastPayload = null;
      ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();

      // 2. Iterate through the streaming response from the server.
      for (AdaptMessageResponse responsePart : serverStream) {
        // Update the cache with any state changes from the server.
        responsePart.getStateUpdatesMap().forEach(attachmentsCache::put);

        // If we've already seen a payload, write it to the body stream.
        // This keeps `lastPayload` always one step behind, holding the most recent part.
        if (lastPayload != null) {
          lastPayload.writeTo(bodyStream);
        }
        lastPayload = responsePart.getPayload();
      }

      // 3. After the loop, `lastPayload` holds the final message part (the header).
      //    If it's the *only* part, the body will be empty.
      if (lastPayload == null) {
        return serverErrorResponse(
            streamId, "No response received from the server."); // No response payloads at all.
      }

      // 4. Assemble the final response: header first, then the rest of the body.
      ByteArrayOutputStream finalResponseStream = new ByteArrayOutputStream();
      lastPayload.writeTo(finalResponseStream);
      bodyStream.writeTo(finalResponseStream);

      return finalResponseStream.toByteArray();
    } catch (IOException | RuntimeException e) {
      // 5. Catch both gRPC and stream-writing errors in a single block.
      LOG.error("Error during gRPC call or stream processing for streamId: {}", streamId, e);
      return serverErrorResponse(streamId, e.getMessage());
    }
  }

  AttachmentsCache getAttachmentsCache() {
    return attachmentsCache;
  }
}

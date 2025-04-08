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

import com.google.api.gax.rpc.ServerStream;
import com.google.protobuf.ByteString;
import com.google.spanner.adapter.v1.AdaptMessageRequest;
import com.google.spanner.adapter.v1.AdaptMessageResponse;
import com.google.spanner.adapter.v1.AdapterClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/** Wraps an {@link AdapterClient} to manage gRPC communication with the Adapter service. */
final class AdapterClientWrapper {

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
   * @return An {@link Optional} containing the byte array payload of the adapter's response, or
   *     {@link Optional#empty()} if no response is received.
   */
  Optional<byte[]> sendGrpcRequest(byte[] payload, Map<String, String> attachments) {

    // TODO: Add RLS headers.
    AdaptMessageRequest request =
        AdaptMessageRequest.newBuilder()
            .setName(sessionManager.getSession().getName())
            .setProtocol("cassandra")
            .putAllAttachments(attachments)
            .setPayload(ByteString.copyFrom(payload))
            .build();

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      ServerStream<AdaptMessageResponse> serverStream =
          adapterClient.adaptMessageCallable().call(request);

      // TODO: Implement response stitching.
      for (AdaptMessageResponse adaptMessageResponse : serverStream) {
        adaptMessageResponse.getStateUpdatesMap().forEach(attachmentsCache::put);
        outputStream.write(adaptMessageResponse.getPayload().toByteArray());
      }

      if (outputStream.size() > 0) {
        return Optional.of(outputStream.toByteArray());
      }
    } catch (IOException | RuntimeException e) {
      // Any error in getting the AdaptMessageResponse should be reported back to the client.
      return Optional.of(serverErrorResponse(e.getMessage()));
    }

    return Optional.empty();
  }

  AttachmentsCache getAttachmentsCache() {
    return attachmentsCache;
  }
}

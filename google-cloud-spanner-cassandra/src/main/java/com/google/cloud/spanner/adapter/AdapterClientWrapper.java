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
import java.util.ArrayList;
import java.util.List;
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

    AdaptMessageRequest request =
        AdaptMessageRequest.newBuilder()
            .setName(sessionManager.getSession().getName())
            .setProtocol("cassandra")
            .putAllAttachments(attachments)
            .setPayload(ByteString.copyFrom(payload))
            .build();

    List<ByteString> collectedPayloads = new ArrayList<>();

    try {
      ServerStream<AdaptMessageResponse> serverStream =
          adapterClient.adaptMessageCallable().call(request, context);
      for (AdaptMessageResponse adaptMessageResponse : serverStream) {
        adaptMessageResponse.getStateUpdatesMap().forEach(attachmentsCache::put);
        collectedPayloads.add(adaptMessageResponse.getPayload());
      }
    } catch (RuntimeException e) {
      LOG.error("Error executing AdaptMessage request: ", e);
      // Any error in getting the AdaptMessageResponse should be reported back to the client.
      return serverErrorResponse(streamId, e.getMessage());
    }

    if (collectedPayloads.isEmpty()) {
      return serverErrorResponse(
          streamId, "No response received from the server."); // No response payloads at all.
    }

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final int numPayloads = collectedPayloads.size();
      // In case of multiple responses, the last response contains the header. So write it first.
      outputStream.write(collectedPayloads.get(numPayloads - 1).toByteArray());

      // Then write the remaining responses.
      for (int i = 0; i < numPayloads - 1; i++) {
        outputStream.write(collectedPayloads.get(i).toByteArray());
      }
      return outputStream.toByteArray();
    } catch (IOException e) {
      LOG.error("Error stitching chunked payloads: ", e);
      return serverErrorResponse(streamId, e.getMessage());
    }
  }

  AttachmentsCache getAttachmentsCache() {
    return attachmentsCache;
  }
}

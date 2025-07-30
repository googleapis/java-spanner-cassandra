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

import com.google.api.gax.rpc.ApiCallContext;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * An object used to encapsulate the result of preparing the Adapter payload prior to sending the
 * gRPC request.
 */
public class PreparePayloadResult {
  private ApiCallContext context;
  private int streamId;
  private Map<String, String> attachments;
  private Optional<byte[]> attachmentErrorResponse;
  private static final Map<String, String> EMPTY_ATTACHMENTS = Collections.emptyMap();

  public PreparePayloadResult(
      ApiCallContext context,
      int streamId,
      Map<String, String> attachments,
      Optional<byte[]> attachmentErrorResponse) {
    this.context = context;
    this.streamId = streamId;
    this.attachments = attachments;
    this.attachmentErrorResponse = attachmentErrorResponse;
  }

  public PreparePayloadResult(
      ApiCallContext context, int streamId, Map<String, String> attachments) {
    this(context, streamId, attachments, Optional.empty());
  }

  public PreparePayloadResult(ApiCallContext context, int streamId) {
    this(context, streamId, EMPTY_ATTACHMENTS, Optional.empty());
  }

  public Map<String, String> getAttachments() {
    return attachments;
  }

  public Optional<byte[]> getAttachmentErrorResponse() {
    return attachmentErrorResponse;
  }

  public ApiCallContext getContext() {
    return context;
  }

  int getStreamId() {
    return streamId;
  }
}

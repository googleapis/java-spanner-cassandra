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
import java.util.Map;
import java.util.Optional;

/**
 * An object used to encapsulate the result of preparing the Adapter payload prior to sending the
 * gRPC request.
 */
public class PreparePayloadResult {
  private Optional<byte[]> attachmentErrorResponse;
  private ApiCallContext context;
  private Map<String, String> attachments;

  public PreparePayloadResult(
      Map<String, String> attachments,
      Optional<byte[]> attachmentErrorResponse,
      ApiCallContext context) {
    this.attachments = attachments;
    this.attachmentErrorResponse = attachmentErrorResponse;
    this.context = context;
  }

  public Map<String, String> getAttachments() {
    return attachments;
  }

  public void setAttachments(Map<String, String> attachments) {
    this.attachments = attachments;
  }

  public Optional<byte[]> getAttachmentErrorResponse() {
    return attachmentErrorResponse;
  }

  public void setAttachmentErrorResponse(Optional<byte[]> attachmentErrorResponse) {
    this.attachmentErrorResponse = attachmentErrorResponse;
  }

  public ApiCallContext getContext() {
    return context;
  }

  public void setContext(ApiCallContext context) {
    this.context = context;
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;

/**
 * A custom retry policy for Cloud Spanner's Cassandra API.
 *
 * <p>This policy inspects the error message of ServerError exceptions (which wrap ReadFailure and
 * WriteFailure). If the message contains Spanner-specific transient error strings like
 * "DEADLINE_EXCEEDED" or "ABORTED", it triggers a retry.
 *
 * <p>For all other cases, it delegates the decision to the default retry policy, preserving the
 * standard driver behavior.
 */
public final class SpannerCqlRetryPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerCqlRetryPolicy.class);
  private final RetryPolicy delegate;
  private static final int MAX_RETRIES = 10;

  private static final ImmutableList<String> RETRYABLE_ERROR_MESSAGES =
      ImmutableList.of(
          "HTTP/2 error code: INTERNAL_ERROR",
          "Connection closed with unknown cause",
          "Received unexpected EOS on DATA frame from server",
          "stream terminated by RST_STREAM",
          "Authentication backend internal server error. Please retry.",
          "DEADLINE_EXCEEDED",
          "ABORTED",
          "RESOURCE_EXHAUSTED",
          "UNAVAILABLE");

  /**
   * Constructor that the driver will invoke.
   *
   * @param context The driver context.
   * @param profileName The name of the execution profile this policy is for.
   */
  public SpannerCqlRetryPolicy(DriverContext context, String profileName) {
    // We delegate to the default policy for all non-Spanner-specific cases.
    this.delegate = new DefaultRetryPolicy(context, profileName);
  }

  /**
   * Checks if the given exception is a Spanner-specific transient error that is safe to retry. This
   * includes gRPC transport errors, connection issues, and standard Spanner transient errors like
   * ABORTED or UNAVAILABLE.
   */
  private boolean IsRetryableSpannerError(CoordinatorException e) {
    if (!(e instanceof WriteFailureException || e instanceof ReadFailureException)) {
      return false;
    }
    return e.getMessage() != null
        && RETRYABLE_ERROR_MESSAGES.stream().anyMatch(e.getMessage()::contains);
  }

  @Override
  public RetryDecision onErrorResponse(Request request, CoordinatorException e, int retryCount) {
    // The Spanner proxy embeds the gRPC error message in the message string of WriteFailure and
    // ReadFailure frame.
    // We check for transient gRPC errors that are safe to retry.
    if (!IsRetryableSpannerError(e)) {
      // For any other error, fall back to the default driver behavior.
      return delegate.onErrorResponse(request, e, retryCount);
    }

    String errorMessage = e.getMessage();
    if (retryCount > MAX_RETRIES) {
      LOG.error(
          "Request with Spanner-specific transient error failed after hitting max retries ({})."
              + " Last error: {}",
          MAX_RETRIES,
          errorMessage);
      return RetryDecision.RETHROW;
    }
    LOG.warn(
        "Spanner-specific transient error detected: '{}'. Retrying query (attempt {}).",
        errorMessage,
        retryCount + 1);
    // Retry on the same node since Spanner is interpreted as a single node to Cassandra
    // driver.
    return RetryDecision.RETRY_SAME;
  }

  // --- Delegate all other methods to the default policy ---
  @Override
  public RetryDecision onReadTimeout(
      Request request,
      ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    return delegate.onReadTimeout(request, cl, blockFor, received, dataPresent, retryCount);
  }

  @Override
  public RetryDecision onWriteTimeout(
      Request request,
      ConsistencyLevel cl,
      WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    return delegate.onWriteTimeout(request, cl, writeType, blockFor, received, retryCount);
  }

  @Override
  public RetryDecision onUnavailable(
      Request request, ConsistencyLevel cl, int required, int alive, int retryCount) {
    return delegate.onUnavailable(request, cl, required, alive, retryCount);
  }

  @Override
  public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount) {
    return delegate.onRequestAborted(request, error, retryCount);
  }

  @Override
  public void close() {
    delegate.close();
  }
}

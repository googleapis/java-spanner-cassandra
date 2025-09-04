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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A custom retry policy for Cloud Spanner's Cassandra API. */
public final class SpannerCqlRetryPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerCqlRetryPolicy.class);

  private static final int DEFAULT_MAX_RETRY_COUNT = 5;

  private static final String RETRYING_ON_READ_TIMEOUT =
      "[{}] Retrying on read timeout (retries: {})";
  private static final String RETRYING_ON_WRITE_TIMEOUT =
      "[{}] Retrying on write timeout (retries: {})";
  private static final String RETRYING_ON_UNAVAILABLE =
      "[{}] Retrying on unavailable exception (retries: {})";
  private static final String RETRYING_ON_ABORTED =
      "[{}] Retrying on aborted request (retries: {})";
  private static final String RETRYING_ON_ERROR = "[{}] Retrying on node error (retries: {})";

  private final String logPrefix;

  public SpannerCqlRetryPolicy(DriverContext context, String profileName) {
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries up to 5 times.
   */
  @Override
  public RetryDecision onReadTimeout(
      Request request,
      ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {

    return retryOrRethrow(retryCount, RETRYING_ON_READ_TIMEOUT);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries up to 5 times.
   */
  @Override
  public RetryDecision onWriteTimeout(
      Request request,
      ConsistencyLevel cl,
      WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    return retryOrRethrow(retryCount, RETRYING_ON_WRITE_TIMEOUT);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries up to 5 times.
   */
  @Override
  public RetryDecision onUnavailable(
      Request request, ConsistencyLevel cl, int required, int alive, int retryCount) {
    return retryOrRethrow(retryCount, RETRYING_ON_UNAVAILABLE);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries up to 5 times.
   */
  @Override
  public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount) {
    return retryOrRethrow(retryCount, RETRYING_ON_ABORTED);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries up to 5 times.
   */
  @Override
  public RetryDecision onErrorResponse(
      Request request, CoordinatorException error, int retryCount) {
    if (error instanceof ReadFailureException || error instanceof WriteFailureException) {
      return RetryDecision.RETHROW;
    }
    return retryOrRethrow(retryCount, RETRYING_ON_ERROR);
  }

  @Override
  public void close() {
    // nothing to do
  }

  private RetryDecision retryOrRethrow(int retryCount, String errorMsg) {
    final RetryDecision decision =
        (retryCount < DEFAULT_MAX_RETRY_COUNT) ? RetryDecision.RETRY_SAME : RetryDecision.RETHROW;
    if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
      LOG.trace(errorMsg, logPrefix, retryCount);
    }
    return decision;
  }
}

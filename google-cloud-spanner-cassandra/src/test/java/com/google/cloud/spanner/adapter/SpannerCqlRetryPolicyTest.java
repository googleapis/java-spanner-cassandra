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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public class SpannerCqlRetryPolicyTest {

  private static final String PROFILE_NAME = "default";
  private static final int MAX_RETRIES = 5;

  @Mock private DriverContext mockContext;
  @Mock private Request mockRequest;
  @Mock private CoordinatorException mockCoordinatorException;
  @Mock private ReadFailureException mockReadFailureException;
  @Mock private WriteFailureException mockWriteFailureException;

  private SpannerCqlRetryPolicy retryPolicy;
  private final int retryCount;

  @Parameters(name = "retryCount: {0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{0}, {1}, {4}});
  }

  public SpannerCqlRetryPolicyTest(int retryCount) {
    this.retryCount = retryCount;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockContext.getSessionName()).thenReturn("test-session");
    retryPolicy = new SpannerCqlRetryPolicy(mockContext, PROFILE_NAME);
  }

  // --- General Retry Tests ---

  @Test
  public void onReadTimeout_shouldRetry_whenRetryCountIsLessThanMax() {
    RetryDecision decision =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.ALL, 1, 1, true, retryCount);
    assertThat(decision).isEqualTo(RetryDecision.RETRY_SAME);
  }

  @Test
  public void onWriteTimeout_shouldRetry_whenRetryCountIsLessThanMax() {
    RetryDecision decision =
        retryPolicy.onWriteTimeout(
            mockRequest, ConsistencyLevel.ALL, WriteType.BATCH, 1, 1, retryCount);
    assertThat(decision).isEqualTo(RetryDecision.RETRY_SAME);
  }

  @Test
  public void onUnavailable_shouldRetry_whenRetryCountIsLessThanMax() {
    RetryDecision decision =
        retryPolicy.onUnavailable(mockRequest, ConsistencyLevel.ALL, 1, 1, retryCount);
    assertThat(decision).isEqualTo(RetryDecision.RETRY_SAME);
  }

  @Test
  public void onRequestAborted_shouldRetry_whenRetryCountIsLessThanMax() {
    RetryDecision decision =
        retryPolicy.onRequestAborted(mockRequest, new RuntimeException("test error"), retryCount);
    assertThat(decision).isEqualTo(RetryDecision.RETRY_SAME);
  }

  // --- Specific onErrorResponse Tests ---

  @Test
  public void onErrorResponse_shouldRetry_forGenericErrorAndRetryCountLessThanMax() {
    RetryDecision decision =
        retryPolicy.onErrorResponse(mockRequest, mockCoordinatorException, retryCount);
    assertThat(decision).isEqualTo(RetryDecision.RETRY_SAME);
  }

  @Test
  public void onErrorResponse_shouldRethrow_forReadFailureException() {
    RetryDecision decision = retryPolicy.onErrorResponse(mockRequest, mockReadFailureException, 0);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void onErrorResponse_shouldRethrow_forWriteFailureException() {
    RetryDecision decision = retryPolicy.onErrorResponse(mockRequest, mockWriteFailureException, 0);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void onReadTimeout_shouldRethrow_whenRetryCountEqualsMax() {
    RetryDecision decision =
        retryPolicy.onReadTimeout(mockRequest, ConsistencyLevel.ALL, 1, 1, true, MAX_RETRIES);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void onWriteTimeout_shouldRethrow_whenRetryCountEqualsMax() {
    RetryDecision decision =
        retryPolicy.onWriteTimeout(
            mockRequest, ConsistencyLevel.ALL, WriteType.BATCH, 1, 1, MAX_RETRIES);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void onUnavailable_shouldRethrow_whenRetryCountEqualsMax() {
    RetryDecision decision =
        retryPolicy.onUnavailable(mockRequest, ConsistencyLevel.ALL, 1, 1, MAX_RETRIES);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void onRequestAborted_shouldRethrow_whenRetryCountEqualsMax() {
    RetryDecision decision =
        retryPolicy.onRequestAborted(mockRequest, new RuntimeException("test error"), MAX_RETRIES);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void onErrorResponse_shouldRethrow_forGenericErrorAndRetryCountEqualsMax() {
    RetryDecision decision =
        retryPolicy.onErrorResponse(mockRequest, mockCoordinatorException, MAX_RETRIES);
    assertThat(decision).isEqualTo(RetryDecision.RETHROW);
  }

  @Test
  public void constructor_withNullContext_shouldNotThrowException() {
    new SpannerCqlRetryPolicy(null, PROFILE_NAME);
  }
}

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

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.session.Request;

@RunWith(JUnit4.class)
public class SpannerCqlRetryPolicyTest {

  private SpannerCqlRetryPolicy policy;
  private Request request;
  private Node node;

  @Before
  public void setUp() {
    DriverContext context = mock(DriverContext.class);
    policy = new SpannerCqlRetryPolicy(context, "default");
    request = mock(Request.class);
    node = mock(Node.class);
    EndPoint endpoint = mock(EndPoint.class);
    when(node.getEndPoint()).thenReturn(endpoint);
    when(endpoint.resolve()).thenReturn(new InetSocketAddress("localhost", 9042));
  }

  @Test
  public void testOnErrorResponse_writeFailure_retryable() {
    WriteFailureException e = mock(WriteFailureException.class);
    when(e.getMessage()).thenReturn("Spanner UNAVAILABLE");
    RetryDecision decision = policy.onErrorResponse(request, e, 0);
    assertEquals(RetryDecision.RETRY_SAME, decision);
  }

  @Test
  public void testOnErrorResponse_readFailure_retryable() {
    ReadFailureException e = mock(ReadFailureException.class);
    when(e.getMessage()).thenReturn("Spanner txn ABORTED");
    RetryDecision decision = policy.onErrorResponse(request, e, 0);
    assertEquals(RetryDecision.RETRY_SAME, decision);
  }

  @Test
  public void testOnErrorResponse_maxRetriesExceeded() {
    ReadFailureException e = mock(ReadFailureException.class);
    when(e.getMessage()).thenReturn("Spanner txn ABORTED");
    RetryDecision decision = policy.onErrorResponse(request, e, 11);
    assertEquals(RetryDecision.RETHROW, decision);
  }

  @Test
  public void testOnErrorResponse_nonRetryableError() {
    ReadFailureException e = mock(ReadFailureException.class);
    when(e.getMessage()).thenReturn("Spanner crashed");
    RetryPolicy mockDelegate = mock(RetryPolicy.class);
    when(mockDelegate.onErrorResponse(request, e, 0)).thenReturn(RetryDecision.RETHROW);
    RetryDecision decision = policy.onErrorResponse(request, e, 0);
    assertEquals(RetryDecision.RETHROW, decision);
  }
}

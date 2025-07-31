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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around {@link CqlSession} that manages the lifecycle of an {@link Adapter}.
 *
 * <p>This class ensures that the associated Adapter is stopped when the session is closed.
 */
public final class SpannerCqlSession implements CqlSession {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerCqlSession.class);
  private final CqlSession delegate;
  private final Adapter adapter;

  /**
   * Returns a builder to create a new instance.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   *
   * @return {@code SpannerCqlSessionBuilder} to create a new instance.
   */
  public static SpannerCqlSessionBuilder builder() {
    return new SpannerCqlSessionBuilder();
  }

  /**
   * Constructs a new SpannerCqlSession.
   *
   * @param delegate The delegate CqlSession.
   * @param adapter The Adapter instance.
   */
  SpannerCqlSession(CqlSession delegate, Adapter adapter) {
    this.delegate = delegate;
    this.adapter = adapter;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return delegate.getName();
  }

  /** {@inheritDoc} */
  @Override
  public Metadata getMetadata() {
    return delegate.getMetadata();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isSchemaMetadataEnabled() {
    return delegate.isSchemaMetadataEnabled();
  }

  /** {@inheritDoc} */
  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(Boolean newValue) {
    return delegate.setSchemaMetadataEnabled(newValue);
  }

  /** {@inheritDoc} */
  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return delegate.refreshSchemaAsync();
  }

  /** {@inheritDoc} */
  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return delegate.checkSchemaAgreementAsync();
  }

  /** {@inheritDoc} */
  @Override
  public DriverContext getContext() {
    return delegate.getContext();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<CqlIdentifier> getKeyspace() {
    return delegate.getKeyspace();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Metrics> getMetrics() {
    return delegate.getMetrics();
  }

  /** {@inheritDoc} */
  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType) {
    return delegate.execute(request, resultType);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Stops the Adapter after closing the delegate session.
   */
  @Override
  public CompletionStage<Void> closeFuture() {
    return delegate.closeFuture().thenCompose(v -> stopAdapterAsync());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Stops the Adapter after closing the delegate session.
   */
  @Override
  public CompletionStage<Void> closeAsync() {
    return delegate.closeAsync().thenCompose(v -> stopAdapterAsync());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Stops the Adapter after force-closing the delegate session.
   */
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return delegate.forceCloseAsync().thenCompose(v -> stopAdapterAsync());
  }

  private CompletionStage<Void> stopAdapterAsync() {
    return CompletableFuture.runAsync(
        () -> {
          try {
            adapter.stop();
          } catch (IOException e) {
            LOG.error("Error stopping adapter.", e);
          }
        });
  }
}

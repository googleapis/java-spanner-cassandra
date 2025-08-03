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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

/**
 * A thread-safe attachments cache maintained across all connections and requests.
 *
 * <p>It uses a Guava {@link com.google.common.cache.Cache} for efficient caching and thread-safe
 * access.
 */
final class AttachmentsCache {

  /** The value part of an entry in the attachments cache. */
  public static class CacheValue {
    private final Map<String, String> attachments;
    private final boolean isRead;

    public CacheValue(Map<String, String> attachments, boolean isRead) {
      this.attachments = ImmutableMap.copyOf(attachments);
      this.isRead = isRead;
    }

    public Map<String, String> getAttachments() {
      return attachments;
    }

    public boolean isRead() {
      return isRead;
    }
  }

  private final Cache<ByteBuffer, CacheValue> cache;

  /**
   * Constructs a new GlobalState with the specified maximum cache size.
   *
   * @param size The maximum number of entries the cache can hold.
   */
  AttachmentsCache(long size) {
    this.cache = CacheBuilder.newBuilder().maximumSize(size).build();
  }

  /**
   * Stores a key-value pair in the cache.
   *
   * @param key The key with which to associate the specified value.
   * @param val The value to be associated with the specified key.
   */
  void put(ByteBuffer key, CacheValue val) {
    cache.put(key, val);
  }

  /**
   * Retrieves the value associated with the specified key from the cache.
   *
   * @param key The key whose associated value is to be returned.
   * @return An {@link Optional} containing the {@link CacheValue} associated with the key, if
   *     present, otherwise an empty {@code Optional}.
   */
  Optional<CacheValue> get(ByteBuffer key) {
    return Optional.ofNullable(cache.getIfPresent(key));
  }
}

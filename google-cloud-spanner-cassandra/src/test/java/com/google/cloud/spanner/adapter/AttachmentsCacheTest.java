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

import com.google.cloud.spanner.adapter.AttachmentsCache.CacheValue;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public final class AttachmentsCacheTest {

  public AttachmentsCacheTest() {}

  @Test
  public void putAndGet() {
    AttachmentsCache attachmentsCache = new AttachmentsCache(100);
    ByteBuffer key1 = ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8));
    Map<String, String> attachments = ImmutableMap.of("a", "b");
    CacheValue value1 = new CacheValue(attachments, true);
    attachmentsCache.put(key1, value1);

    Optional<CacheValue> retrievedValue = attachmentsCache.get(key1);

    assertThat(retrievedValue.isPresent()).isTrue();
    assertThat(retrievedValue.get()).isEqualTo(value1);
    assertThat(retrievedValue.get().getAttachments()).isEqualTo(attachments);
    assertThat(retrievedValue.get().isRead()).isTrue();
  }

  @Test
  public void getNonExistentKey() {
    AttachmentsCache attachmentsCache = new AttachmentsCache(100);
    ByteBuffer nonExistentKey = ByteBuffer.wrap("nonExistentKey".getBytes(StandardCharsets.UTF_8));

    Optional<CacheValue> nonExistent = attachmentsCache.get(nonExistentKey);

    assertThat(nonExistent.isPresent()).isFalse();
  }

  @Test
  public void lruPolicy() {
    AttachmentsCache attachmentsCache = new AttachmentsCache(2);
    ByteBuffer key1 = ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8));
    CacheValue value1 = new CacheValue(ImmutableMap.of("v", "1"), true);
    ByteBuffer key2 = ByteBuffer.wrap("key2".getBytes(StandardCharsets.UTF_8));
    CacheValue value2 = new CacheValue(ImmutableMap.of("v", "2"), true);
    ByteBuffer key3 = ByteBuffer.wrap("key3".getBytes(StandardCharsets.UTF_8));
    CacheValue value3 = new CacheValue(ImmutableMap.of("v", "3"), true);

    attachmentsCache.put(key1, value1);
    attachmentsCache.put(key2, value2);
    // Access key1 to make it the most recently used.
    attachmentsCache.get(key1);
    // This should evict key2.
    attachmentsCache.put(key3, value3);

    Optional<CacheValue> retrievedValue1 = attachmentsCache.get(key1);
    Optional<CacheValue> retrievedValue2 = attachmentsCache.get(key2);
    Optional<CacheValue> retrievedValue3 = attachmentsCache.get(key3);

    assertThat(retrievedValue1.isPresent()).isTrue();
    assertThat(retrievedValue2.isPresent()).isFalse();
    assertThat(retrievedValue3.isPresent()).isTrue();
  }
}

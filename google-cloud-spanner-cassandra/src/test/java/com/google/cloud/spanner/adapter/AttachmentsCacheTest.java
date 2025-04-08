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

import java.util.Optional;
import org.junit.Test;

public final class AttachmentsCacheTest {

  public AttachmentsCacheTest() {}

  @Test
  public void putAndGet() {
    AttachmentsCache AttachmentsCache = new AttachmentsCache(100);
    AttachmentsCache.put("key1", "value1");

    Optional<String> value1 = AttachmentsCache.get("key1");

    assertThat(value1.isPresent());
    assertThat(value1.get()).isEqualTo("value1");
  }

  @Test
  public void getNonExistentKey() {
    AttachmentsCache AttachmentsCache = new AttachmentsCache(100);

    Optional<String> nonExistent = AttachmentsCache.get("nonExistentKey");

    assertThat(nonExistent.isPresent()).isFalse();
  }

  @Test
  public void lruPolicy() {
    AttachmentsCache AttachmentsCache = new AttachmentsCache(2);
    AttachmentsCache.put("key1", "value1");
    AttachmentsCache.put("key2", "value2");
    AttachmentsCache.put("key3", "value3");
    Optional<String> value1 = AttachmentsCache.get("key1");
    Optional<String> value2 = AttachmentsCache.get("key2");
    Optional<String> value3 = AttachmentsCache.get("key3");

    assertThat(value1.isPresent()).isFalse();
    assertThat(value2.isPresent());
    assertThat(value2.get()).isEqualTo("value2");
    assertThat(value3.isPresent());
    assertThat(value3.get()).isEqualTo("value3");
  }
}

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

import static com.google.cloud.spanner.adapter.util.StringUtils.startsWith;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public final class StringUtilsTest {
  public StringUtilsTest() {}

  @Test
  public void stringStartsWith() {
    assertThat(startsWith("AbC", "aB")).isTrue();
    assertThat(startsWith("  AbC", "aB")).isTrue();

    assertThat(startsWith("abc", "ax")).isFalse();
    assertThat(startsWith("abc", null)).isFalse();
    assertThat(startsWith(null, "x")).isFalse();
    assertThat(startsWith(null, null)).isFalse();
    assertThat(startsWith("", "x")).isFalse();
  }
}

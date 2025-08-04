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

import static com.google.cloud.spanner.adapter.util.ThreadFactoryUtil.tryCreateVirtualThreadFactory;
import static com.google.cloud.spanner.adapter.util.ThreadFactoryUtil.tryCreateVirtualThreadPerTaskExecutor;
import static com.google.common.truth.Truth.assertThat;

import java.util.concurrent.ThreadFactory;
import org.junit.Test;

public class ThreadFactoryUtilTest {

  @Test
  public void testCreateThreadFactory() throws Exception {
    if (isJava21OrHigher()) {
      ThreadFactory virtualFactory = tryCreateVirtualThreadFactory("test-thread");
      assertThat(virtualFactory).isNotNull();
    } else {
      assertThat(tryCreateVirtualThreadFactory("test-thread")).isNull();
    }
  }

  @Test
  public void testTryCreateVirtualThreadPerTaskExecutor() {
    if (isJava21OrHigher()) {
      assertThat(tryCreateVirtualThreadPerTaskExecutor("test-virtual-thread")).isNotNull();
    } else {
      assertThat(tryCreateVirtualThreadPerTaskExecutor("test-virtual-thread")).isNull();
    }
  }

  private static boolean isJava21OrHigher() {
    String[] versionElements = System.getProperty("java.version").split("\\.");
    int majorVersion = Integer.parseInt(versionElements[0]);
    // Java 1.8 (Java 8) and lower used the format 1.8 etc.
    // Java 9 and higher use the format 9.x
    if (majorVersion == 1) {
      majorVersion = Integer.parseInt(versionElements[1]);
    }
    return majorVersion >= 21;
  }
}

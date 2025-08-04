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

package com.google.cloud.spanner.adapter.util;

import com.google.api.core.InternalApi;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nullable;

/** Utility class for creating a thread factory for daemon or virtual threads. */
@InternalApi
public final class ThreadFactoryUtil {

  /**
   * Tries to create a {@link ThreadFactory} that creates virtual threads. Returns null if virtual
   * threads are not supported on this JVM.
   */
  @InternalApi
  @Nullable
  public static ThreadFactory tryCreateVirtualThreadFactory(String baseNameFormat) {
    try {
      Class<?> threadBuilderClass = Class.forName("java.lang.Thread$Builder");
      Method ofVirtualMethod = Thread.class.getDeclaredMethod("ofVirtual");
      Object virtualBuilder = ofVirtualMethod.invoke(null);
      Method nameMethod = threadBuilderClass.getDeclaredMethod("name", String.class, long.class);
      virtualBuilder = nameMethod.invoke(virtualBuilder, baseNameFormat + "-", 0);
      Method factoryMethod = threadBuilderClass.getDeclaredMethod("factory");
      return (ThreadFactory) factoryMethod.invoke(virtualBuilder);
    } catch (ClassNotFoundException | NoSuchMethodException ignore) {
      return null;
    } catch (InvocationTargetException | IllegalAccessException e) {
      // Java 20 supports virtual threads as an experimental feature. It will throw an
      // UnsupportedOperationException if experimental features have not been enabled.
      if (e.getCause() instanceof UnsupportedOperationException) {
        return null;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Tries to create an {@link ExecutorService} that creates a new virtual thread for each task that
   * it runs. Creating a new virtual thread is the recommended way to create executors using virtual
   * threads, instead of creating a pool of virtual threads. Returns null if virtual threads are not
   * supported on this JVM.
   */
  @InternalApi
  @Nullable
  public static ExecutorService tryCreateVirtualThreadPerTaskExecutor(String baseNameFormat) {
    ThreadFactory factory = tryCreateVirtualThreadFactory(baseNameFormat);
    if (factory != null) {
      try {
        Method newThreadPerTaskExecutorMethod =
            Executors.class.getDeclaredMethod("newThreadPerTaskExecutor", ThreadFactory.class);
        return (ExecutorService) newThreadPerTaskExecutorMethod.invoke(null, factory);
      } catch (NoSuchMethodException ignore) {
        return null;
      } catch (InvocationTargetException | IllegalAccessException e) {
        // Java 20 supports virtual threads as an experimental feature. It will throw an
        // UnsupportedOperationException if experimental features have not been enabled.
        if (e.getCause() instanceof UnsupportedOperationException) {
          return null;
        }
        throw new RuntimeException(e);
      }
    }
    return null;
  }
}

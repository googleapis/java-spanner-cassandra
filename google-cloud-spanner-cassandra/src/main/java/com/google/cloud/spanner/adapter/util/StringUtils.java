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

/**
 * Utility class for handling {@link String} objects.
 *
 * <p>This class cannot be instantiated.
 */
public final class StringUtils {

  private StringUtils() {
    throw new IllegalStateException("Utility class cannot be instantiated");
  }

  /**
   * Returns true if `input` starts with `prefix`, excluding leading whitespaces and doing a
   * case-insensitive comparison.
   */
  public static boolean startsWith(String input, String prefix) {
    if (input == null || prefix == null) {
      return false;
    }

    // Efficiently trim leading whitespace without creating a new String object if unnecessary.
    int i = 0;
    while (i < input.length() && Character.isWhitespace(input.charAt(i))) {
      i++;
    }

    // Check if the remaining part starts with `prefix` (case-insensitive).
    // Using regionMatches for efficiency as it avoids substring creation.
    return input.regionMatches(true, i, prefix, 0, prefix.length());
  }
}

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
package com.google.cloud.spanner.adapter.opentelemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BuiltInMetricsProviderTest {
  @Test
  public void testGenerateClientHashWithSimpleUid() {
    String clientUid = "testClient";
    verifyHash(BuiltInMetricsProvider.generateClientHash(clientUid));
  }

  @Test
  public void testGenerateClientHashWithEmptyUid() {
    String clientUid = "";
    verifyHash(BuiltInMetricsProvider.generateClientHash(clientUid));
  }

  @Test
  public void testGenerateClientHashWithNullUid() {
    String clientUid = null;
    verifyHash(BuiltInMetricsProvider.generateClientHash(clientUid));
  }

  @Test
  public void testGenerateClientHashWithLongUid() {
    String clientUid = "aVeryLongUniqueClientIdentifierThatIsUnusuallyLong";
    verifyHash(BuiltInMetricsProvider.generateClientHash(clientUid));
  }

  @Test
  public void testGenerateClientHashWithSpecialCharacters() {
    String clientUid = "273d60f2-5604-42f1-b687-f5f1b975fd07@2316645@test#";
    verifyHash(BuiltInMetricsProvider.generateClientHash(clientUid));
  }

  private static void verifyHash(String hash) {
    // Check if the hash length is 6
    assertEquals(hash.length(), 6);
    // Check if the hash is in the range [000000, 0003ff]
    long hashValue = Long.parseLong(hash, 16); // Convert hash from hex to decimal
    assertTrue(hashValue >= 0 && hashValue <= 0x3FF);
  }
}

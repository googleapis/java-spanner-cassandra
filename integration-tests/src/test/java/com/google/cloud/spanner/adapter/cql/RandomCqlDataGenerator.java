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

package com.google.cloud.spanner.adapter.cql;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RandomCqlDataGenerator {

  private static final String ALPHANUMERIC_CHARS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private static final String SPECIAL_CHARS = "!@#$%^&*()_+-=[]{}|;':\",.<>/?`~";
  private static final String ALL_CHARS = ALPHANUMERIC_CHARS + SPECIAL_CHARS;

  static final Random random = new Random(); // Consider ThreadLocalRandom for concurrent access

  /** Generates a random UUID. */
  public static UUID randomUUID() {
    return UUID.randomUUID();
  }

  /**
   * Generates a random text string of specified length.
   *
   * @param length The exact length of the string.
   */
  public static String randomText(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(ALL_CHARS.charAt(random.nextInt(ALL_CHARS.length())));
    }
    return sb.toString();
  }

  /**
   * Generates a random text string within a specified length range.
   *
   * @param minLength The minimum length of the string.
   * @param maxLength The maximum length of the string.
   */
  public static String randomText(int minLength, int maxLength) {
    int length = ThreadLocalRandom.current().nextInt(minLength, maxLength + 1);
    return randomText(length);
  }

  /** Generates a random 32-bit integer. */
  public static int randomInt() {
    return random.nextInt();
  }

  /** Generates a random 64-bit integer (long). */
  public static long randomBigint() {
    return random.nextLong();
  }

  /** Generates a random boolean. */
  public static boolean randomBoolean() {
    return random.nextBoolean();
  }

  /**
   * Generates a random BigDecimal with a random scale and precision.
   *
   * @param maxScale The maximum number of digits after the decimal point.
   * @param maxPrecision The maximum total number of digits.
   */
  public static BigDecimal randomBigDecimal(int maxScale, int maxPrecision) {
    if (maxScale < 0) throw new IllegalArgumentException("maxScale must be non-negative");
    if (maxPrecision < 1) throw new IllegalArgumentException("maxPrecision must be at least 1");

    // Generate a random BigInteger for the unscaled value
    int numDigits = ThreadLocalRandom.current().nextInt(1, maxPrecision + 1);
    StringBuilder sb = new StringBuilder();
    if (numDigits > 0) {
      sb.append(ThreadLocalRandom.current().nextInt(1, 10)); // First digit 1-9
    }
    for (int i = 1; i < numDigits; i++) {
      sb.append(ThreadLocalRandom.current().nextInt(0, 10)); // Subsequent digits 0-9
    }
    BigInteger unscaledValue = new BigInteger(sb.toString());

    // Randomly choose scale
    int scale = ThreadLocalRandom.current().nextInt(0, maxScale + 1);

    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Generates a random Instant within a reasonable past/future range.
   *
   * @param daysOffset The number of days to offset from current time (positive for future, negative
   *     for past).
   */
  public static Instant randomInstant(int daysOffset) {
    long currentMillis = Instant.now().toEpochMilli();
    long randomMillisOffset =
        ThreadLocalRandom.current().nextLong(0, TimeUnit.DAYS.toMillis(Math.abs(daysOffset)));
    if (daysOffset < 0) {
      return Instant.ofEpochMilli(currentMillis - randomMillisOffset);
    } else {
      return Instant.ofEpochMilli(currentMillis + randomMillisOffset);
    }
  }

  /** Generates a random Instant around now (within +/- 365 days). */
  public static Instant randomInstant() {
    return randomInstant(ThreadLocalRandom.current().nextInt(-365, 366));
  }

  /**
   * Generates a random LocalDate.
   *
   * @param yearsOffset The number of years to offset from current year.
   */
  public static LocalDate randomLocalDate(int yearsOffset) {
    long currentEpochDay = LocalDate.now().toEpochDay();
    long randomDayOffset = ThreadLocalRandom.current().nextLong(0, 365L * Math.abs(yearsOffset));
    if (yearsOffset < 0) {
      return LocalDate.ofEpochDay(currentEpochDay - randomDayOffset);
    } else {
      return LocalDate.ofEpochDay(currentEpochDay + randomDayOffset);
    }
  }

  /** Generates a random LocalDate around today (within +/- 5 years). */
  public static LocalDate randomLocalDate() {
    return randomLocalDate(ThreadLocalRandom.current().nextInt(-5, 6));
  }

  /** Generates a random LocalTime. */
  public static LocalTime randomLocalTime() {
    long nanosOfDay = ThreadLocalRandom.current().nextLong(0, 24L * 60 * 60 * 1_000_000_000);
    return LocalTime.ofNanoOfDay(nanosOfDay);
  }

  /** Generates a random CqlDuration. */
  public static CqlDuration randomCqlDuration() {
    return CqlDuration.newInstance(
        random.nextInt(12), // months
        random.nextInt(30), // days
        random.nextLong());
  }

  /** Generates a random INET address (IPv4). */
  public static InetAddress randomInetAddress() {
    try {
      byte[] ip = new byte[4];
      random.nextBytes(ip);
      // Avoid multicast and special addresses for simplicity in testing
      ip[0] = (byte) ThreadLocalRandom.current().nextInt(1, 224); // Exclude 0, 224-255
      return InetAddress.getByAddress(ip);
    } catch (Exception e) {
      // Should not happen with valid byte array length
      throw new RuntimeException("Failed to generate random InetAddress", e);
    }
  }

  /**
   * Generates a random BLOB (ByteBuffer).
   *
   * @param minBytes The minimum number of bytes.
   * @param maxBytes The maximum number of bytes.
   */
  public static ByteBuffer randomBlob(int minBytes, int maxBytes) {
    int length = ThreadLocalRandom.current().nextInt(minBytes, maxBytes + 1);
    byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  /**
   * Generates a random Set of a given type.
   *
   * @param <T> The type of elements in the set.
   * @param minElements The minimum number of elements.
   * @param maxElements The maximum number of elements.
   * @param elementGenerator A supplier function to generate individual elements.
   */
  public static <T> Set<T> randomSet(
      int minElements, int maxElements, Supplier<T> elementGenerator) {
    int numElements = ThreadLocalRandom.current().nextInt(minElements, maxElements + 1);
    Set<T> set = new HashSet<>();
    for (int i = 0; i < numElements; i++) {
      set.add(elementGenerator.get());
    }
    return set;
  }

  /**
   * Generates a random List of a given type.
   *
   * @param <T> The type of elements in the list.
   * @param minElements The minimum number of elements.
   * @param maxElements The maximum number of elements.
   * @param elementGenerator A supplier function to generate individual elements.
   */
  public static <T> List<T> randomList(
      int minElements, int maxElements, Supplier<T> elementGenerator) {
    int numElements = ThreadLocalRandom.current().nextInt(minElements, maxElements + 1);
    List<T> list = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      list.add(elementGenerator.get());
    }
    return list;
  }

  /**
   * Generates a random Map with specified key and value types.
   *
   * @param <K> The type of keys in the map.
   * @param <V> The type of values in the map.
   * @param minEntries The minimum number of entries.
   * @param maxEntries The maximum number of entries.
   * @param keyGenerator A supplier function to generate keys.
   * @param valueGenerator A supplier function to generate values.
   */
  public static <K, V> Map<K, V> randomMap(
      int minEntries, int maxEntries, Supplier<K> keyGenerator, Supplier<V> valueGenerator) {
    int numEntries = ThreadLocalRandom.current().nextInt(minEntries, maxEntries + 1);
    Map<K, V> map = new HashMap<>();
    for (int i = 0; i < numEntries; i++) {
      map.put(keyGenerator.get(), valueGenerator.get());
    }
    return map;
  }
}

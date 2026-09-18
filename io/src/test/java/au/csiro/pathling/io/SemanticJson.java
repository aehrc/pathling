/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/**
 * The comparison the round trip is judged by: object key order ignored, array order significant,
 * and numbers compared lexically (FR-016).
 *
 * <p>Lexical comparison is what makes the comparison worth making. A number is read as a {@link
 * java.math.BigDecimal} that keeps its scale, so {@code 1.50} and {@code 1.5} are different values
 * here although they are the same quantity; Jackson strips trailing zeros by default and would
 * quietly make them equal, so the node factory is configured against it.
 *
 * <p>The kind of a value is compared before the value is, so that a decimal returned as the string
 * {@code "1.50"} is a difference rather than a match. Without that, a serialiser that quoted every
 * decimal would pass a comparison built on text.
 */
final class SemanticJson {

  /** Reads numbers exactly, keeping the scale the source wrote them at. */
  @Nonnull
  private static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
          .build();

  private SemanticJson() {}

  /**
   * Parses a JSON document into a tree.
   *
   * @param json the document
   * @return the tree
   */
  @Nonnull
  static JsonNode parse(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json);
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException("Not a JSON document: " + json, e);
    }
  }

  /**
   * Returns a description of the first place two trees differ, or empty where they are semantically
   * equal.
   *
   * @param path the path the comparison has reached, which roots the description
   * @param expected the tree the source parsed to
   * @param actual the tree the round trip produced
   * @return the difference, or empty where there is none
   */
  @Nonnull
  static Optional<String> difference(
      @Nonnull final String path,
      @Nonnull final JsonNode expected,
      @Nonnull final JsonNode actual) {
    final Optional<String> kind = kindDifference(path, expected, actual);
    if (kind.isPresent()) {
      return kind;
    }
    if (expected.isObject()) {
      return objectDifference(path, (ObjectNode) expected, (ObjectNode) actual);
    }
    if (expected.isArray()) {
      return arrayDifference(path, expected, actual);
    }
    if (expected.isNumber()) {
      return value(path, expected.decimalValue().toString(), actual.decimalValue().toString());
    }
    return value(path, expected.asText(), actual.asText());
  }

  /**
   * Compares the kind of two values, which is what keeps a number returned as text from matching
   * the number it was written as.
   */
  @Nonnull
  private static Optional<String> kindDifference(
      @Nonnull final String path,
      @Nonnull final JsonNode expected,
      @Nonnull final JsonNode actual) {
    final String expectedKind = kindOf(expected);
    final String actualKind = kindOf(actual);
    return expectedKind.equals(actualKind)
        ? Optional.empty()
        : Optional.of(
            path
                + ": expected a "
                + expectedKind
                + " and found a "
                + actualKind
                + " ("
                + expected
                + " against "
                + actual
                + ")");
  }

  @Nonnull
  private static String kindOf(@Nonnull final JsonNode node) {
    if (node.isObject()) {
      return "object";
    }
    if (node.isArray()) {
      return "array";
    }
    if (node.isNumber()) {
      return "number";
    }
    if (node.isBoolean()) {
      return "boolean";
    }
    return node.isNull() ? "null" : "string";
  }

  @Nonnull
  private static Optional<String> objectDifference(
      @Nonnull final String path,
      @Nonnull final ObjectNode expected,
      @Nonnull final ObjectNode actual) {
    final TreeSet<String> expectedKeys = new TreeSet<>(names(expected));
    final TreeSet<String> actualKeys = new TreeSet<>(names(actual));
    if (!expectedKeys.equals(actualKeys)) {
      final TreeSet<String> missing = new TreeSet<>(expectedKeys);
      missing.removeAll(actualKeys);
      final TreeSet<String> unexpected = new TreeSet<>(actualKeys);
      unexpected.removeAll(expectedKeys);
      return Optional.of(path + ": missing " + missing + ", unexpected " + unexpected);
    }
    for (final String name : expectedKeys) {
      final Optional<String> difference =
          difference(path + "." + name, expected.get(name), actual.get(name));
      if (difference.isPresent()) {
        return difference;
      }
    }
    return Optional.empty();
  }

  @Nonnull
  private static Optional<String> arrayDifference(
      @Nonnull final String path,
      @Nonnull final JsonNode expected,
      @Nonnull final JsonNode actual) {
    if (expected.size() != actual.size()) {
      return Optional.of(
          path + ": expected " + expected.size() + " elements and found " + actual.size());
    }
    for (int i = 0; i < expected.size(); i++) {
      final Optional<String> difference =
          difference(path + "[" + i + "]", expected.get(i), actual.get(i));
      if (difference.isPresent()) {
        return difference;
      }
    }
    return Optional.empty();
  }

  @Nonnull
  private static Optional<String> value(
      @Nonnull final String path, @Nonnull final String expected, @Nonnull final String actual) {
    return expected.equals(actual)
        ? Optional.empty()
        : Optional.of(path + ": expected " + expected + " and found " + actual);
  }

  @Nonnull
  private static List<String> names(@Nonnull final ObjectNode node) {
    final List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    return names;
  }
}

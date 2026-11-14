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
package au.csiro.pathling.legacy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.UncheckedIOException;
import java.util.Comparator;

/**
 * Compares FHIR JSON documents semantically: the order of an object's keys does not matter, the
 * order of an array's items does, and numbers are compared by value, so that {@code 1.50} equals
 * {@code 1.5}.
 */
final class SemanticJson {

  @Nonnull
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  /** Orders two nodes as equal where they are numbers of the same value, or are equal nodes. */
  @Nonnull
  private static final Comparator<JsonNode> NUMERIC =
      (left, right) -> {
        if (left.isNumber() && right.isNumber()) {
          return left.decimalValue().compareTo(right.decimalValue());
        }
        return left.equals(right) ? 0 : 1;
      };

  private SemanticJson() {}

  /**
   * Parses a JSON document.
   *
   * @param json the document
   * @return its tree
   */
  @Nonnull
  static JsonNode parse(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Whether two documents are semantically equal.
   *
   * @param expected one document
   * @param actual the other
   * @return whether they are equal
   */
  static boolean equal(@Nonnull final JsonNode expected, @Nonnull final JsonNode actual) {
    return expected.equals(NUMERIC, actual);
  }
}

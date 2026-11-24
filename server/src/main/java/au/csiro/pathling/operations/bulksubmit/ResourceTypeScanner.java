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

package au.csiro.pathling.operations.bulksubmit;

import au.csiro.pathling.shaded.com.fasterxml.jackson.core.JsonFactory;
import au.csiro.pathling.shaded.com.fasterxml.jackson.core.JsonParser;
import au.csiro.pathling.shaded.com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Finds resources in an NDJSON file whose type differs from the type declared for the file.
 *
 * @author John Grimes
 */
public final class ResourceTypeScanner {

  private static final String RESOURCE_TYPE = "resourceType";

  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private ResourceTypeScanner() {}

  /**
   * Counts the resources in an NDJSON stream whose top-level {@code resourceType} is not the
   * expected type.
   *
   * @param ndjson the NDJSON content, which is read to the end but not closed
   * @param expectedType the resource type declared for the file
   * @return the number of resources of each unexpected type, ordered by type
   * @throws IOException if the stream cannot be read
   */
  @Nonnull
  public static Map<String, Long> countUnexpectedTypes(
      @Nonnull final InputStream ndjson, @Nonnull final String expectedType) throws IOException {
    final Map<String, Long> counts = new TreeMap<>();
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(ndjson, StandardCharsets.UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      readResourceType(line)
          .filter(type -> !type.equals(expectedType))
          .ifPresent(type -> counts.merge(type, 1L, Long::sum));
    }
    return counts;
  }

  /**
   * Reads the top-level {@code resourceType} of a single NDJSON line. The line is parsed as a
   * stream so that only the fields preceding {@code resourceType} are read, which in FHIR JSON is
   * usually none.
   *
   * @param line the NDJSON line
   * @return the resource type, or empty if the line is not a JSON object with a string resourceType
   */
  @Nonnull
  private static Optional<String> readResourceType(@Nonnull final String line) {
    try (final JsonParser parser = JSON_FACTORY.createParser(line)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        return Optional.empty();
      }
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final String fieldName = parser.currentName();
        final JsonToken value = parser.nextToken();
        if (RESOURCE_TYPE.equals(fieldName)) {
          return value == JsonToken.VALUE_STRING ? Optional.of(parser.getText()) : Optional.empty();
        }
        parser.skipChildren();
      }
      return Optional.empty();
    } catch (final IOException e) {
      // Lines that are not valid JSON are left for the import to handle.
      return Optional.empty();
    }
  }
}

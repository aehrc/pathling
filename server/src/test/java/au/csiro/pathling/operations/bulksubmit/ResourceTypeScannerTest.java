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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ResourceTypeScanner}.
 *
 * @author John Grimes
 */
class ResourceTypeScannerTest {

  private static Map<String, Long> scan(final String ndjson, final String expectedType)
      throws IOException {
    final InputStream input = new ByteArrayInputStream(ndjson.getBytes(StandardCharsets.UTF_8));
    return ResourceTypeScanner.countUnexpectedTypes(input, expectedType);
  }

  @Test
  void returnsEmptyWhenAllResourcesMatchExpectedType() throws IOException {
    final String ndjson =
        """
        {"resourceType":"AllergyIntolerance","id":"a1"}
        {"resourceType":"AllergyIntolerance","id":"a2"}
        """;

    assertThat(scan(ndjson, "AllergyIntolerance")).isEmpty();
  }

  @Test
  void returnsEmptyForEmptyInput() throws IOException {
    assertThat(scan("", "Patient")).isEmpty();
  }

  @Test
  void countsResourcesOfUnexpectedType() throws IOException {
    // This reproduces the file from the issue: two matching resources and one Patient.
    final String ndjson =
        """
        {"resourceType":"AllergyIntolerance","id":"a1"}
        {"resourceType":"AllergyIntolerance","id":"a2"}
        {"resourceType":"Patient","id":"p1"}
        """;

    assertThat(scan(ndjson, "AllergyIntolerance")).containsExactly(Map.entry("Patient", 1L));
  }

  @Test
  void countsEachUnexpectedTypeSeparatelyInAlphabeticalOrder() throws IOException {
    final String ndjson =
        """
        {"resourceType":"Patient","id":"p1"}
        {"resourceType":"Condition","id":"c1"}
        {"resourceType":"AllergyIntolerance","id":"a1"}
        {"resourceType":"Patient","id":"p2"}
        {"resourceType":"Patient","id":"p3"}
        """;

    // The order is deterministic so that the reported outcomes are stable.
    assertThat(scan(ndjson, "AllergyIntolerance"))
        .containsExactly(Map.entry("Condition", 1L), Map.entry("Patient", 3L));
  }

  @Test
  void findsResourceTypeWhenItIsNotTheFirstField() throws IOException {
    final String ndjson =
        """
        {"id":"p1","meta":{"versionId":"1"},"resourceType":"Patient"}
        """;

    assertThat(scan(ndjson, "Observation")).containsExactly(Map.entry("Patient", 1L));
  }

  @Test
  void ignoresResourceTypeOfNestedResources() throws IOException {
    // A contained resource precedes the top-level resourceType, and must not be mistaken for it.
    final String ndjson =
        """
        {"contained":[{"resourceType":"Patient","id":"p1"}],"resourceType":"Observation"}
        """;

    assertThat(scan(ndjson, "Observation")).isEmpty();
  }

  @Test
  void ignoresBlankLines() throws IOException {
    final String ndjson = "\n   \n{\"resourceType\":\"Patient\"}\n\n";

    assertThat(scan(ndjson, "Patient")).isEmpty();
  }

  @Test
  void ignoresLinesThatAreNotValidJson() throws IOException {
    // Malformed lines are left for the import to deal with, as they were before this check.
    final String ndjson =
        """
        {"resourceType":"Patient"
        not json at all
        {"resourceType":"Condition"}
        """;

    assertThat(scan(ndjson, "Patient")).containsExactly(Map.entry("Condition", 1L));
  }

  @Test
  void ignoresTruncatedLinesEvenWhenResourceTypeIsComplete() throws IOException {
    // The resourceType can be read before the point at which the line is cut off, but the line is
    // still not valid JSON, so it is left for the import to handle.
    final String ndjson =
        """
        {"resourceType":"Condition","id":"c1"
        {"resourceType":"Condition","id":"c2"}}
        {"resourceType":"Condition"} trailing
        """;

    assertThat(scan(ndjson, "Patient")).isEmpty();
  }

  @Test
  void ignoresLinesWithoutResourceType() throws IOException {
    final String ndjson =
        """
        {"id":"x1"}
        """;

    assertThat(scan(ndjson, "Patient")).isEmpty();
  }

  @Test
  void ignoresLinesWhereResourceTypeIsNotAString() throws IOException {
    final String ndjson =
        """
        {"resourceType":42}
        {"resourceType":{"value":"Condition"}}
        {"resourceType":null}
        """;

    assertThat(scan(ndjson, "Patient")).isEmpty();
  }

  @Test
  void ignoresLinesThatAreNotJsonObjects() throws IOException {
    final String ndjson =
        """
        [{"resourceType":"Condition"}]
        "Condition"
        """;

    assertThat(scan(ndjson, "Patient")).isEmpty();
  }

  @Test
  void handlesWindowsLineEndings() throws IOException {
    final String ndjson = "{\"resourceType\":\"Patient\"}\r\n{\"resourceType\":\"Condition\"}\r\n";

    assertThat(scan(ndjson, "Patient")).containsExactly(Map.entry("Condition", 1L));
  }
}

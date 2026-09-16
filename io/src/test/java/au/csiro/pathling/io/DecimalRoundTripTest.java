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

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trips the lexical forms a decimal can take (FR-002, FR-016).
 *
 * <p>Each of these is lost by anything that parses the value as a number on the way through, and
 * each is lost differently: a trailing zero to normalisation, an exponent to rendering, a long
 * digit count to the precision of a fixed-point type or a double. The comparison is lexical, and it
 * distinguishes a number from its text, so a decimal returned quoted fails here rather than passing
 * as a string that happens to match.
 */
class DecimalRoundTripTest {

  /**
   * The forms under test, as the identifier of the resource carrying each. A leading {@code +} is
   * absent because JSON does not admit one, so the leading sign under test is the minus.
   */
  @Nonnull
  private static final List<String> FORMS =
      List.of(
          "1.50",
          "-1.50",
          "1e2",
          "1.0e-7",
          "0.000000001",
          "1234567890123456789012345678901234567890.5");

  @Test
  void preservesEveryLexicalFormOfADecimal(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus = directory.resolve("observations.ndjson");
    Files.write(corpus, documents());

    RoundTripHarness.unconditional().assertRoundTrip("Observation", corpus);
  }

  @Test
  void returnsADecimalAsANumberRatherThanAsText(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus = directory.resolve("observations.ndjson");
    Files.write(
        corpus,
        List.of(
            "{\"resourceType\":\"Observation\",\"id\":\"quantity\",\"status\":\"final\","
                + "\"valueQuantity\":{\"value\":1.50,\"unit\":\"1.50\"}}"));

    // The unit carries the same characters as a string, so a serialiser that quoted the decimal
    // would make the two indistinguishable and this comparison is what separates them.
    assertEquals(0, RoundTripHarness.unconditional().assertRoundTrip("Observation", corpus));
  }

  @Nonnull
  private static List<String> documents() {
    return Stream.iterate(0, index -> index + 1)
        .limit(FORMS.size())
        .map(
            index ->
                "{\"resourceType\":\"Observation\",\"id\":\"form-"
                    + index
                    + "\",\"status\":\"final\",\"valueQuantity\":{\"value\":"
                    + FORMS.get(index)
                    + "}}")
        .collect(Collectors.toList());
  }
}

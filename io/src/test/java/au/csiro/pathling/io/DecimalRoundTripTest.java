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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trips the lexical forms a decimal can take (FR-002, FR-016).
 *
 * <p>A decimal is read and written through a double, so none of these forms survives textually: a
 * trailing zero is dropped, an exponent is rendered as a double renders it, and a long digit count
 * is cut to the precision of a double. Each must still survive numerically (decision 68). The
 * comparison distinguishes a number from its text, so a decimal returned quoted fails here rather
 * than passing as a string that happens to match.
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
  void keepsEveryFormOfADecimalNumericallyEqual(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus = directory.resolve("observations.ndjson");
    Files.write(corpus, documents());

    final RoundTripOutcome outcome =
        RoundTripHarness.unconditional().assertRoundTrip("Observation", corpus);

    // One form is not counted: 1.0e-7 comes back as 1.0E-7, the same digits and scale with the
    // exponent marker in the other case, which the count cannot see once both are parsed. That it
    // is
    // not textually identical is asserted on the stored text below.
    assertEquals(
        FORMS.size() - 1,
        outcome.getNumericOnly(),
        "every other form matched numerically and not textually");
  }

  @Test
  void storesNoFormTextuallyIdenticalToItsSource(@TempDir @Nonnull final Path directory)
      throws IOException {
    // The other half of the guarantee: numeric equality is all that is kept. A form that survived
    // textually would mean the value never went through a double, and this test would need to say
    // why.
    final Path corpus = directory.resolve("observations.ndjson");
    Files.write(corpus, documents());
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read("Observation", corpus.toString())
            .select("id", "valueQuantity.value");

    for (final Row row : stored.collectAsList()) {
      final String source =
          FORMS.get(Integer.parseInt(row.getString(0).substring("form-".length())));
      final String text = row.getString(1);
      assertNotEquals(source, text, "the stored text of " + source);
      assertEquals(Double.parseDouble(source), Double.parseDouble(text), "the value of " + source);
    }
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
    assertEquals(
        0,
        RoundTripHarness.unconditional()
            .assertRoundTrip("Observation", corpus)
            .getPrimitiveMetadata());
  }

  @Test
  void writesADecimalUnquotedAndAStringQuoted(@TempDir @Nonnull final Path directory) {
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Observation\",\"id\":\"quantity\",\"status\":\"final\","
                + "\"valueQuantity\":{\"value\":1.50,\"unit\":\"1.50\"}}");
    final Dataset<Row> stored = TransformFixtures.reader().read("Observation", path);

    final String document =
        TransformFixtures.writer().write("Observation", stored).collectAsList().get(0);

    // The two carried the same characters, and only the definitions say which of them is a number.
    assertTrue(document.contains("\"value\":1.5"), document);
    assertTrue(document.contains("\"unit\":\"1.50\""), document);
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
        .toList();
  }
}

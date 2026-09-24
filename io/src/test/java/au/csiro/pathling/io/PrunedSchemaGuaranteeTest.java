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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.transform.NonConformantContent;
import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

/**
 * Tests FR-017 on a pruned schema (US2, scenario 3).
 *
 * <p>The round trip holds subject to FR-016's exception list, and the dense bounds do not apply,
 * because the schema is fitted to the data (FR-044). The one exception the source here exercises is
 * primitive element ids and extensions, which are not written until M5. That carve-out is exercised
 * rather than assumed: the source carries such content, the round trip does not return it, and the
 * loss is reported as a finding.
 *
 * <p>The dense half of FR-017 was withdrawn with the reporting of what the dense bounds drop
 * (decision 68), and the dense mode itself arrives in M6 (decision 69).
 */
class PrunedSchemaGuaranteeTest {

  /** Carries an extension on a complex element and the id and extensions of a primitive one. */
  @Nonnull
  private static final String PATIENT_WITH_METADATA =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"male\","
          + "\"birthDate\":\"1980-01-01\","
          + "\"_birthDate\":{\"id\":\"bd1\",\"extension\":[{\"url\":\"http://example.com/when\","
          + "\"valueString\":\"about then\"}]},"
          + "\"extension\":[{\"url\":\"http://example.com/note\",\"valueString\":\"a note\"}]}";

  /** Carries an extension, which the dense bounds would drop and a pruned schema stores. */
  @Nonnull
  private static final String PATIENT_WITH_EXTENSION =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"male\","
          + "\"extension\":[{\"url\":\"http://example.com/note\",\"valueString\":\"a note\"}]}";

  @Test
  void holdsUnconditionallyOnAPrunedSchemaButForPrimitiveMetadata(
      @TempDir @Nonnull final Path directory) throws IOException {
    final Path corpus = corpus(directory, PATIENT_WITH_METADATA);
    final RoundTripHarness harness = RoundTripHarness.excludingPrimitiveMetadata();

    final int excluded = harness.assertRoundTrip("Patient", corpus).getPrimitiveMetadata();

    assertEquals(1, excluded, "the carve-out is exercised rather than incidental");
    final List<NonConformantContent> findings = harness.findings("Patient", corpus);
    assertTrue(
        findings.stream().allMatch(NonConformantContent::isPrimitiveMetadata),
        "nothing but the carve-out is lost on a pruned schema: " + findings);
    assertEquals(
        List.of("Patient._birthDate"),
        findings.stream().map(NonConformantContent::getPath).toList());
  }

  @Test
  void reportsNothingOfTheBoundsOnAPrunedSchema(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus = corpus(directory, PATIENT_WITH_EXTENSION);

    final List<NonConformantContent> findings =
        RoundTripHarness.unconditional().findings("Patient", corpus);

    assertTrue(
        findings.isEmpty(),
        "the bounds do not apply to a schema fitted to the data (FR-044): " + findings);
  }

  // The exceptions for ignored content, each asserted by the count the harness returns.

  @Test
  void excludesContentTheDefinitionsDoNotDescribe(@TempDir @Nonnull final Path directory)
      throws IOException {
    // Undescribed content at the root and inside an array item, neither of which leaves its element
    // with nothing in it. An item that ignoring leaves empty is the case decision 71 puts outside
    // the layout's input, and it is asserted on the output below rather than through the harness.
    final Path corpus =
        corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"bogusField\":\"x\","
                + "\"name\":[{\"bogusChild\":\"y\",\"family\":\"Jones\"},{\"family\":\"Smith\"}]}");

    final RoundTripOutcome outcome =
        RoundTripHarness.unconditional()
            .excludingIgnoredContent()
            .assertRoundTrip("Patient", corpus);

    assertEquals(2, outcome.getIgnoredContent());
  }

  @Test
  void writesAnEmptyObjectWhereIgnoringContentEmptiesAnArrayItem(
      @TempDir @Nonnull final Path directory) {
    // What deferring the pruning costs (decision 71). The first name holds only undescribed
    // content, so nothing of it is stored, and the item is written as an empty object rather than
    // dropped as decision 66 had it. The document that comes back is not conformant FHIR, and the
    // input that produced it is outside what the layout carries.
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\","
                + "\"name\":[{\"bogusChild\":\"y\"},{\"family\":\"Smith\"}]}");
    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);

    final String document =
        TransformFixtures.writer().write("Patient", stored).collectAsList().get(0);

    assertTrue(document.contains("\"name\":[{},{\"family\":\"Smith\"}]"), document);
  }

  @Test
  void failsWhereIgnoredContentIsNotExcluded(@TempDir @Nonnull final Path directory)
      throws IOException {
    // Without the option the same loss is a difference, so the exclusion is what admits it.
    final Path corpus =
        corpus(directory, "{\"resourceType\":\"Patient\",\"id\":\"1\",\"bogusField\":\"x\"}");

    final RoundTripHarness harness = RoundTripHarness.unconditional();
    assertThrows(AssertionFailedError.class, () -> harness.assertRoundTrip("Patient", corpus));
  }

  @Test
  void excludesEveryValueOfAColumnRetypedByOneOfThem(@TempDir @Nonnull final Path directory)
      throws IOException {
    // A conformant 2 is lost because a sibling 1.5 re-typed the column it shares.
    final Path corpus =
        corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"multipleBirthInteger\":2}",
            "{\"resourceType\":\"Patient\",\"id\":\"2\",\"multipleBirthInteger\":1.5}");

    final RoundTripOutcome outcome =
        RoundTripHarness.unconditional()
            .excludingIgnoredContent()
            .assertRoundTrip("Patient", corpus);

    assertEquals(2, outcome.getIgnoredContent());
  }

  @Test
  void excludesTheWhitespaceInsideABase64Value(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus =
        corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\","
                + "\"photo\":[{\"data\":\"aGVs bG8g d29y bGQ=\"},{\"data\":\"aGVsbG8=\"}]}");

    final RoundTripOutcome outcome =
        RoundTripHarness.unconditional().assertRoundTrip("Patient", corpus);

    assertEquals(1, outcome.getBase64Whitespace(), "only the wrapped value is changed");
  }

  @Nonnull
  private static Path corpus(@Nonnull final Path directory, @Nonnull final String... documents)
      throws IOException {
    final Path corpus = directory.resolve("patients.ndjson");
    Files.write(corpus, List.of(documents));
    return corpus;
  }
}

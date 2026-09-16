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
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.transform.NonConformantContent;
import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the two halves of FR-017 (US2, scenarios 3 and 4).
 *
 * <p>On a pruned schema the round trip is unconditional, because the schema is fitted to the data
 * and the bounds do not apply to it — but for primitive element ids and extensions, which are not
 * written until M5. That carve-out is exercised here rather than assumed: the source carries such
 * content, the round trip does not return it, and the loss is reported as a finding.
 *
 * <p>On a dense schema the guarantee holds only within the configured bounds, and content those
 * bounds would drop has to be detectable. Detectable means reachable as a value: a warning in a log
 * is not something a caller can act on.
 */
class DenseBoundsDetectionTest {

  /** Carries an extension on a complex element and the id and extensions of a primitive one. */
  @Nonnull
  private static final String PATIENT_WITH_METADATA =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"male\","
          + "\"birthDate\":\"1980-01-01\","
          + "\"_birthDate\":{\"id\":\"bd1\",\"extension\":[{\"url\":\"http://example.com/when\","
          + "\"valueString\":\"about then\"}]},"
          + "\"extension\":[{\"url\":\"http://example.com/note\",\"valueString\":\"a note\"}]}";

  /**
   * Carries nothing the bounds reach, so the dense round trip is as unconditional as the pruned.
   */
  @Nonnull
  private static final String PATIENT_WITHIN_BOUNDS =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"active\":true,\"gender\":\"male\","
          + "\"birthDate\":\"1980-01-01\",\"multipleBirthInteger\":2,"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\",\"Elizabeth\"]}],"
          + "\"managingOrganization\":{\"reference\":\"Organization/1\"}}";

  /** Carries an extension, which the bounds drop because extensions are off by default. */
  @Nonnull
  private static final String PATIENT_WITH_EXTENSION =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"male\","
          + "\"extension\":[{\"url\":\"http://example.com/note\",\"valueString\":\"a note\"}]}";

  /** Nests a reference within a reference, which the nesting bound stops the derivation at. */
  @Nonnull
  private static final String PATIENT_WITH_NESTED_REFERENCE =
      "{\"resourceType\":\"Patient\",\"id\":\"1\","
          + "\"managingOrganization\":{\"identifier\":{\"value\":\"o1\","
          + "\"assigner\":{\"display\":\"an assigner\"}}}}";

  @Test
  void holdsUnconditionallyOnAPrunedSchemaButForPrimitiveMetadata(
      @TempDir @Nonnull final Path directory) throws IOException {
    final Path corpus = corpus(directory, PATIENT_WITH_METADATA);
    final RoundTripHarness harness = RoundTripHarness.excludingPrimitiveMetadata();

    final int excluded = harness.assertRoundTrip("Patient", corpus);

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
  void detectsContentTheExtensionBoundWouldDrop(@TempDir @Nonnull final Path directory)
      throws IOException {
    final List<NonConformantContent> findings =
        denseFindings(corpus(directory, PATIENT_WITH_EXTENSION));

    assertEquals(
        List.of("Patient.extension"),
        findings.stream()
            .filter(NonConformantContent::isOutsideDenseBounds)
            .map(NonConformantContent::getPath)
            .toList());
  }

  @Test
  void detectsContentTheNestingBoundWouldDrop(@TempDir @Nonnull final Path directory)
      throws IOException {
    final List<NonConformantContent> findings =
        denseFindings(corpus(directory, PATIENT_WITH_NESTED_REFERENCE));

    assertEquals(
        List.of("Patient.managingOrganization.identifier.assigner"),
        findings.stream()
            .filter(NonConformantContent::isOutsideDenseBounds)
            .map(NonConformantContent::getPath)
            .toList());
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

  @Test
  void holdsWithinTheBoundsOnADenseSchema(@TempDir @Nonnull final Path directory)
      throws IOException {
    final Path corpus = corpus(directory, PATIENT_WITHIN_BOUNDS);

    final int excluded =
        RoundTripHarness.unconditional()
            .withConfiguration(SchemaConfiguration.builder().denseSchema(true).build())
            .assertRoundTrip("Patient", corpus);

    assertEquals(0, excluded, "this source carries no primitive id or extension content");
  }

  @Nonnull
  private static List<NonConformantContent> denseFindings(@Nonnull final Path corpus) {
    return RoundTripHarness.unconditional()
        .withConfiguration(SchemaConfiguration.builder().denseSchema(true).build())
        .findings("Patient", corpus);
  }

  @Nonnull
  private static Path corpus(@Nonnull final Path directory, @Nonnull final String document)
      throws IOException {
    final Path corpus = directory.resolve("patients.ndjson");
    Files.write(corpus, List.of(document));
    return corpus;
  }
}

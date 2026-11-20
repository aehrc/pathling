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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

/**
 * Tests FR-017 on a pruned schema (US2, scenario 3).
 *
 * <p>The round trip holds subject to FR-016's exception list, and the dense bounds do not apply,
 * because the schema is fitted to the data (FR-044). The one exception the source here exercises is
 * primitive element ids and extensions, which are not written until ticket 17. That exception is
 * exercised rather than assumed: the source carries such content, and the round trip admits its
 * loss only where the harness strips it.
 *
 * <p>The dense mode itself arrives with ticket 18.
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

    assertEquals(1, excluded, "the exception is exercised rather than incidental");
  }

  @Test
  void appliesNoBoundsOnAPrunedSchema(@TempDir @Nonnull final Path directory) throws IOException {
    // The bounds do not apply to a schema fitted to the data (FR-044), so an extension the dense
    // bounds would drop comes back with nothing excluded.
    final Path corpus = corpus(directory, PATIENT_WITH_EXTENSION);

    RoundTripHarness.unconditional().assertRoundTrip("Patient", corpus);
  }

  @Test
  void failsWhereAnExceptionIsNotExcluded(@TempDir @Nonnull final Path directory)
      throws IOException {
    // A contained resource is a listed exception, so the same conformant source fails the round
    // trip without the option and passes with it, which shows that the stripping is what admits
    // the loss.
    final Path corpus =
        corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"contained\":"
                + "[{\"resourceType\":\"Observation\",\"id\":\"o\",\"status\":\"final\","
                + "\"code\":{\"text\":\"weight\"}}],\"gender\":\"male\"}");

    final RoundTripHarness harness = RoundTripHarness.unconditional();
    assertThrows(AssertionFailedError.class, () -> harness.assertRoundTrip("Patient", corpus));
    harness.excludingContainedResources().assertRoundTrip("Patient", corpus);
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

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

import jakarta.annotation.Nonnull;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Round-trips resources through Parquet files on disk, which is what the layout is for.
 *
 * <p>Every other round trip in this module goes from the transform to the serialiser in memory, so
 * it proves those two and not the storage between them. This one writes the stored dataset, reads
 * it back and asserts the schema survived before returning it to JSON. It is deliberately a sample
 * rather than the whole of either corpus: the corpora are what prove the transform, and the
 * question here is only whether Parquet preserves what the transform produced, which the
 * specification examples with the most structural corners answer as well as all of them would.
 */
class PersistedRoundTripTest {

  @ParameterizedTest
  @CsvSource({
    // The example carrying the most corners: every choice type, base64, nested extensions.
    "Patient",
    // Decimals with trailing zeros, in money nested inside repeating structures.
    "ExplanationOfBenefit",
    // Structures nested deeply enough that an ordering or nesting fault would show.
    "AuditEvent",
    // An unsigned integer, money and base64 beside a contained resource.
    "CoverageEligibilityResponse",
    "ActivityDefinition"
  })
  void roundTripsSpecificationExamples(
      @Nonnull final String resourceType, @TempDir @Nonnull final Path directory) {
    RoundTripHarness.excludingPrimitiveMetadata()
        .excludingContainedResources()
        .persistingTo(directory)
        .assertRoundTrip(resourceType, corpus("fhir-spec", resourceType));
  }

  @ParameterizedTest
  @CsvSource({"Patient", "Observation"})
  void roundTripsSyntheaOnAPrunedSchema(
      @Nonnull final String resourceType, @TempDir @Nonnull final Path directory) {
    RoundTripHarness.unconditional()
        .persistingTo(directory)
        .assertRoundTrip(resourceType, corpus("synthea", resourceType));
  }

  @Nonnull
  private static Path corpus(@Nonnull final String source, @Nonnull final String resourceType) {
    final String resource = "/data/" + source + "/R4/" + resourceType + ".ndjson";
    try {
      return Path.of(
          Objects.requireNonNull(PersistedRoundTripTest.class.getResource(resource)).toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("The corpus is not readable: " + resource, e);
    }
  }
}

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
import jakarta.annotation.Nonnull;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Round-trips a Synthea corpus, which is the independent test US2 names: a real dataset, resource
 * by resource, with no FHIRPath evaluation involved.
 *
 * <p>The corpus is per-resource-type newline-delimited JSON, so the exclusion T076 needs for {@code
 * Bundle} does not arise here. FR-017's carve-out for primitive id and extension content is carried
 * all the same, and the number of keys it excludes is asserted rather than assumed: this corpus
 * carries none, so the round trip over it is unconditional, and a corpus that began carrying such
 * content would change the count rather than quietly widen the carve-out.
 */
class SyntheaRoundTripTest {

  @ParameterizedTest
  @ValueSource(strings = {"Patient", "Condition", "Questionnaire", "Observation"})
  void roundTripsTheCorpus(@Nonnull final String resourceType) {
    final Path corpus = corpus(resourceType);
    final RoundTripHarness harness = RoundTripHarness.excludingPrimitiveMetadata();

    final List<NonConformantContent> findings = harness.findings(resourceType, corpus);
    assertTrue(
        findings.isEmpty(), "the corpus carries content this layout does not store: " + findings);

    assertEquals(
        0,
        harness.assertRoundTrip(resourceType, corpus),
        "the corpus carries no primitive id or extension content, so nothing is excluded");
  }

  @Nonnull
  private static Path corpus(@Nonnull final String resourceType) {
    final String resource = "/data/synthea/R4/" + resourceType + ".ndjson";
    try {
      return Path.of(
          Objects.requireNonNull(SyntheaRoundTripTest.class.getResource(resource)).toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("The corpus is not readable: " + resource, e);
    }
  }
}

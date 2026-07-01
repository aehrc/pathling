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

package au.csiro.pathling.sof.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChecksumVerifierTest {

  private static final String CONDITION_SHA =
      "64c58f6cde2353e62b6eefc26a27291ff4e7369e11962564891784bb1f2faf11";
  private static final String OBSERVATION_SHA =
      "3d15d1ac66e72bbf139e4b34a805c7582ca00c7e4d3e886a8fb7e56bdcc55518";

  @Test
  void matchingChecksumsYieldNoDrift() {
    final List<String> drift =
        ChecksumVerifier.verify(
            TestResources.dataRoot().resolve("synthea-clinical").resolve("1").resolve("s"),
            Map.of(
                "Condition.ndjson", CONDITION_SHA,
                "Observation.ndjson", OBSERVATION_SHA));

    assertTrue(drift.isEmpty(), () -> "expected no drift, got: " + drift);
  }

  @Test
  void mismatchingChecksumIsReported() {
    final List<String> drift =
        ChecksumVerifier.verify(
            TestResources.dataRoot().resolve("synthea-clinical").resolve("1").resolve("s"),
            Map.of("Condition.ndjson", "0".repeat(64)));

    assertEquals(1, drift.size());
    assertTrue(drift.get(0).contains("Condition.ndjson"));
    assertTrue(drift.get(0).contains("drift"));
  }

  @Test
  void missingLockedFileIsReported() {
    final List<String> drift =
        ChecksumVerifier.verify(
            TestResources.dataRoot().resolve("synthea-clinical").resolve("1").resolve("s"),
            Map.of("Encounter.ndjson", CONDITION_SHA));

    assertEquals(1, drift.size());
    assertTrue(drift.get(0).contains("Encounter.ndjson"));
    assertTrue(drift.get(0).contains("missing"));
  }
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class CheckfileTest {

  @Test
  void resolvesSiblingBySwappingExtension() {
    final Path sibling = Checkfile.siblingOf(Path.of("/tmp/suite/clinical-flat.json"));
    assertEquals("clinical-flat.check.json", sibling.getFileName().toString());
    assertEquals("suite", sibling.getParent().getFileName().toString());
  }

  @Test
  void parsesAssertionsResourceCountsAndChecksums() {
    final Checkfile checkfile = Checkfile.parseOrEmpty(TestResources.benchmarkFile());

    assertTrue(checkfile.isPresent());
    assertEquals(2, checkfile.expectedCount("condition-flat", "s").orElseThrow());
    assertEquals(2, checkfile.expectedCount("observation-components", "s").orElseThrow());
    assertEquals(1, checkfile.expectedCount("active-conditions", "s").orElseThrow());
    assertTrue(checkfile.expectedCount("condition-flat", "unknown-size").isEmpty());
    assertTrue(checkfile.expectedCount("unknown-case", "s").isEmpty());

    assertEquals(2, checkfile.resourceCounts("s").get("Condition"));
    assertEquals(2, checkfile.resourceCounts("s").get("Observation"));

    assertEquals(
        "64c58f6cde2353e62b6eefc26a27291ff4e7369e11962564891784bb1f2faf11",
        checkfile.fileChecksums("s").get("Condition.ndjson"));
    assertEquals(
        "3d15d1ac66e72bbf139e4b34a805c7582ca00c7e4d3e886a8fb7e56bdcc55518",
        checkfile.fileChecksums("s").get("Observation.ndjson"));
  }

  @Test
  void absentCheckfileIsEmpty() {
    final Path noSibling = TestResources.path("/contract-v2/benchmark-report.schema.json");
    final Checkfile checkfile = Checkfile.parseOrEmpty(noSibling);

    assertFalse(checkfile.isPresent());
    assertTrue(checkfile.expectedCount("condition-flat", "s").isEmpty());
    assertTrue(checkfile.resourceCounts("s").isEmpty());
    assertTrue(checkfile.fileChecksums("s").isEmpty());
  }
}

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

import java.util.List;
import org.junit.jupiter.api.Test;

class BenchmarkFileTest {

  @Test
  void parsesSuiteAndDatasetIdentity() {
    final BenchmarkFile benchmark = BenchmarkFile.parse(TestResources.benchmarkFile());

    assertEquals("clinical-flat", benchmark.getName());
    assertEquals("1", benchmark.getVersion());
    assertEquals("clinical-flat", benchmark.getTitle());
    assertEquals("4.0.1", benchmark.getFhirVersion());

    final BenchmarkDataset dataset = benchmark.getDataset();
    assertEquals("synthea-clinical", dataset.getName());
    assertEquals("1", dataset.getVersion());
    assertEquals("3.2.0", dataset.getSyntheaVersion().orElseThrow());
    assertEquals(List.of("Condition", "Observation"), dataset.getResources());
  }

  @Test
  void parsesCasesWithStableIdAndVariancePermitted() {
    final List<BenchmarkCase> cases = BenchmarkFile.parse(TestResources.benchmarkFile()).getCases();

    assertEquals(3, cases.size());
    assertEquals("condition-flat", cases.get(0).getId());
    assertEquals("Condition", cases.get(0).getResource());
    assertFalse(cases.get(0).isCountVariancePermitted());

    assertEquals("observation-components", cases.get(1).getId());
    assertFalse(cases.get(1).isCountVariancePermitted());

    assertEquals("active-conditions", cases.get(2).getId());
    assertTrue(cases.get(2).isCountVariancePermitted());
  }

  @Test
  void parsesIterations() {
    final BenchmarkFile benchmark = BenchmarkFile.parse(TestResources.benchmarkFile());
    assertEquals(1, benchmark.getWarmup());
    assertEquals(5, benchmark.getMeasurement());
  }
}

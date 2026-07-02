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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Verifies per-case failure isolation: a failing case is recorded and the run continues. */
class FailureIsolationTest {

  @Test
  void aFailingCaseIsRecordedAsExecutionErrorWithMessage() {
    final CaseResult result =
        SofBenchmarkRunner.runIsolated(
            "broken-case",
            () -> {
              throw new RuntimeException("boom: could not evaluate view");
            });

    assertEquals("broken-case", result.getId());
    assertEquals(CaseResult.EXECUTION_ERROR, result.getStatus());
    assertEquals(Optional.of("boom: could not evaluate view"), result.getMessage());
    assertTrue(result.getExecuteExtractSamplesMs().isEmpty());
  }

  @Test
  void aSucceedingCaseIsReturnedUnchanged() {
    final CaseResult measured = new CaseResult("ok-case", "ok", 2L, 2L, List.of(10.0, 11.0), 42.0);

    final CaseResult result = SofBenchmarkRunner.runIsolated("ok-case", () -> measured);

    assertEquals(measured, result);
    assertTrue(result.getMessage().isEmpty());
  }

  @Test
  void oneFailingCaseDoesNotAbortTheRunNorVoidTheOthers() {
    // A message-less RuntimeException still yields a recorded failure (falls back to toString).
    final List<Supplier<CaseResult>> measurements =
        List.of(
            () -> new CaseResult("first", "ok", 2L, 2L, List.of(10.0), 42.0),
            () -> {
              throw new IllegalStateException();
            },
            () -> new CaseResult("third", "count_mismatch", 2L, 3L, List.of(12.0), 42.0));
    final List<String> ids = List.of("first", "second", "third");

    final List<CaseResult> results = new ArrayList<>();
    for (int i = 0; i < measurements.size(); i++) {
      results.add(SofBenchmarkRunner.runIsolated(ids.get(i), measurements.get(i)));
    }

    // Every case is recorded, in order, and the failure did not change the others' statuses.
    assertEquals(
        List.of("first", "second", "third"), results.stream().map(CaseResult::getId).toList());
    assertEquals("ok", results.get(0).getStatus());
    assertEquals(CaseResult.EXECUTION_ERROR, results.get(1).getStatus());
    assertTrue(results.get(1).getMessage().isPresent());
    assertEquals("count_mismatch", results.get(2).getStatus());
  }
}

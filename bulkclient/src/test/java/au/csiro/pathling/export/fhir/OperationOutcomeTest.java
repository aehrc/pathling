/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class OperationOutcomeTest {

  @Test
  void testParseAValidOperationOutcome() {

    final String validOperationOutcomeJson = new JSONObject()
        .put("resourceType", "OperationOutcome")
        .put("issue",
            new JSONArray()
                .put(new JSONObject()
                    .put("severity", "error")
                    .put("code", "transient")
                    .put("diagnostics", "Transient error"))
                .put(new JSONObject()
                    .put("severity", "warning")
                    .put("code", "timeout")
                    .put("diagnostics", "Timeout error"))
        ).toString();

    assertEquals(
        OperationOutcome.builder()
            .issue(List.of(
                OperationOutcome.Issue.builder()
                    .severity("error")
                    .code("transient")
                    .diagnostics("Transient error")
                    .build(),
                OperationOutcome.Issue.builder()
                    .severity("warning")
                    .code("timeout")
                    .diagnostics("Timeout error")
                    .build()
            )).build(),
        OperationOutcome.parse(validOperationOutcomeJson).orElseThrow()
    );
  }

  @Test
  void testParsingIgnoresOtherResourceTypes() {
    final String invalidOperationOutcomeJson = new JSONObject()
        .put("resourceType", "Patient")
        .put("issue",
            new JSONArray()
                .put(new JSONObject()
                    .put("severity", "error")
                    .put("code", "transient")
                    .put("diagnostics", "Transient error"))
        ).toString();

    assertEquals(Optional.empty(), OperationOutcome.parse(invalidOperationOutcomeJson)
    );
  }

  @Test
  void testIsTransientIfAllIssuesTransient() {
    assertTrue(OperationOutcome.builder()
        .issue(List.of(
            OperationOutcome.Issue.builder()
                .severity("error")
                .code("transient")
                .diagnostics("Transient error")
                .build(),
            OperationOutcome.Issue.builder()
                .severity("warning")
                .code("transient")
                .diagnostics("Timeout error")
                .build()
        )).build().isTransient());
  }

  @Test
  void testIsNotTransientIfAnyIssueIsNotTransient() {
    assertFalse(OperationOutcome.builder()
        .issue(List.of(
            OperationOutcome.Issue.builder()
                .severity("error")
                .code("transient")
                .diagnostics("Transient error")
                .build(),
            OperationOutcome.Issue.builder()
                .severity("warning")
                .code("timeout")
                .diagnostics("Timeout error")
                .build()
        )).build().isTransient());
  }
}

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

package au.csiro.pathling.operations.sql;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlOperationError#internalError(String)}.
 *
 * @author John Grimes
 */
class SqlOperationErrorTest {

  @Test
  void internalErrorCarriesA500WithAProcessingIssueAndNoCause() {
    // ErrorHandlingInterceptor unwraps an InternalErrorException to its cause and reprocesses it,
    // so a crafted message survives only when no cause is attached.
    final BaseServerResponseException exception = SqlOperationError.internalError("msg");

    assertThat(exception.getStatusCode()).isEqualTo(500);
    assertThat(exception.getMessage()).isEqualTo("msg");
    assertThat(exception.getCause()).isNull();

    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    final OperationOutcomeIssueComponent issue = outcome.getIssueFirstRep();
    assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.ERROR);
    assertThat(issue.getCode()).isEqualTo(IssueType.PROCESSING);
    assertThat(issue.getDiagnostics()).isEqualTo("msg");
    assertThat(issue.getExpression()).isEmpty();
  }
}

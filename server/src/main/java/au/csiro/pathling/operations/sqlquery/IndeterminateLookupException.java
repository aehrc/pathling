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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.operations.sql.SqlOperationError;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * A terminology lookup whose outcome is unknown: the terminology server answered with a failure, so
 * whether the dependency is of the kind looked up cannot be told. It is a {@code 422 Unprocessable
 * Entity} whose {@code OperationOutcome} carries one {@code invalid} issue.
 *
 * <p>The value set and concept map lookups throw it so that the dependency resolver can keep the
 * failure rather than end resolution: the issue is discarded when a later lookup resolves the
 * dependency, and otherwise reported together with any other kept issue, in lookup order.
 *
 * @author John Grimes
 */
public class IndeterminateLookupException extends UnprocessableEntityException {

  @Serial private static final long serialVersionUID = 4127339868231794245L;

  /** The one issue this failure reports. */
  @Nonnull private final OperationOutcomeIssueComponent issue;

  /**
   * Creates the failure with one {@code invalid} issue.
   *
   * @param expression the parameter at fault, or null when the failure names no parameter
   * @param message the diagnostics message
   */
  public IndeterminateLookupException(
      @Nullable final String expression, @Nonnull final String message) {
    super(message);
    issue = SqlOperationError.issue(IssueType.INVALID, expression, message);
    final OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue(issue);
    setOperationOutcome(outcome);
  }

  /**
   * Returns the one issue this failure reports, so that a caller can combine it with other kept
   * issues into a single {@code OperationOutcome}.
   *
   * @return the issue
   */
  @Nonnull
  public OperationOutcomeIssueComponent getIssue() {
    return issue;
  }
}

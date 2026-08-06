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

import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * Extracts the reportable issues from a thrown HAPI exception, so that a failure raised deep in the
 * resolution chain can be combined with others into a single {@code OperationOutcome}.
 *
 * <p>Exceptions raised by this feature already carry a populated outcome. Those raised by the older
 * shared machinery carry only a message, so an issue is synthesised for them with a code inferred
 * from the status the exception maps to.
 *
 * @author John Grimes
 */
public final class SqlOperationOutcomes {

  /** The {@code expression} value naming the runtime bindings in error outcomes. */
  public static final String PARAMETERS_EXPRESSION = "parameters";

  private SqlOperationOutcomes() {
    // Utility class.
  }

  /**
   * Relabels a preparation failure onto the {@code parameters} part.
   *
   * <p>A 400 raised while preparing a subject that supplied bindings is, in practice, always about
   * those bindings: the structural failures of the artefact itself surface as 404 or 422. The
   * shared SQL machinery predates this feature's error contract and raises them with no outcome of
   * its own, so one is supplied here rather than letting a bare message reach the client.
   *
   * @param exception the failure raised while preparing the subject
   * @return the same exception when it already carries an outcome, otherwise one naming {@code
   *     parameters}
   */
  @Nonnull
  public static InvalidRequestException asBindingFailure(
      @Nonnull final InvalidRequestException exception) {
    if (exception.getOperationOutcome() != null) {
      return exception;
    }
    return SqlOperationError.badRequest(
        IssueType.INVALID, PARAMETERS_EXPRESSION, exception.getMessage());
  }

  /**
   * Returns the issues an exception should contribute to a combined outcome.
   *
   * @param exception the exception thrown
   * @param fallbackExpression the parameter to name when the exception carries no outcome of its
   *     own, or null to name none
   * @return the issues, never empty
   */
  @Nonnull
  public static List<OperationOutcomeIssueComponent> issuesOf(
      @Nonnull final BaseServerResponseException exception,
      @Nullable final String fallbackExpression) {
    if (exception.getOperationOutcome() instanceof final OperationOutcome outcome
        && !outcome.getIssue().isEmpty()) {
      return List.copyOf(outcome.getIssue());
    }
    final String message =
        exception.getMessage() == null
            ? "The request could not be processed."
            : exception.getMessage();
    return List.of(SqlOperationError.issue(inferCode(exception), fallbackExpression, message));
  }

  /** Infers an issue code from the exception type, for an exception carrying no outcome. */
  @Nonnull
  private static IssueType inferCode(@Nonnull final BaseServerResponseException exception) {
    if (exception instanceof ResourceNotFoundException) {
      return IssueType.NOTFOUND;
    }
    if (exception instanceof UnprocessableEntityException
        || exception instanceof InvalidRequestException) {
      return IssueType.INVALID;
    }
    return IssueType.PROCESSING;
  }
}

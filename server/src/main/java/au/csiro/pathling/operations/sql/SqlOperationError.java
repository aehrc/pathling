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

import static java.util.Objects.requireNonNullElse;

import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * Builds the HAPI exceptions that carry the {@code $sql-run} and {@code $sql-export} error
 * contract: a status code, an {@code OperationOutcome} whose issue carries the specified {@code
 * issue.code}, and an {@code expression} naming the parameter at fault.
 *
 * <p>Rejections that concern several parameters at once - the "report every problem in one outcome"
 * rule at export kick-off - are built from a collected list of issues via {@link #of(int, List)}.
 *
 * @author John Grimes
 */
public final class SqlOperationError {

  private SqlOperationError() {
    // Utility class.
  }

  /**
   * Builds a {@code 400 Bad Request} carrying the given issue code and expression.
   *
   * @param code the {@code issue.code} to report
   * @param expression the parameter at fault, or null when the failure names no parameter
   * @param message the diagnostics message
   * @return the exception to throw
   */
  @Nonnull
  public static InvalidRequestException badRequest(
      @Nonnull final IssueType code,
      @Nullable final String expression,
      @Nonnull final String message) {
    final InvalidRequestException exception = new InvalidRequestException(message);
    exception.setOperationOutcome(outcomeOf(List.of(issue(code, expression, message))));
    return exception;
  }

  /**
   * Builds a {@code 404 Not Found} carrying {@code issue.code = not-found} and the given
   * expression.
   *
   * @param expression the parameter at fault, or null when the failure names no parameter
   * @param message the diagnostics message
   * @return the exception to throw
   */
  @Nonnull
  public static ResourceNotFoundException notFound(
      @Nullable final String expression, @Nonnull final String message) {
    final ResourceNotFoundException exception = new ResourceNotFoundException(message);
    exception.setOperationOutcome(
        outcomeOf(List.of(issue(IssueType.NOTFOUND, expression, message))));
    return exception;
  }

  /**
   * Builds a {@code 422 Unprocessable Entity} carrying {@code issue.code = invalid} and the given
   * expression.
   *
   * @param expression the parameter at fault, or null when the failure names no parameter
   * @param message the diagnostics message
   * @return the exception to throw
   */
  @Nonnull
  public static UnprocessableEntityException unprocessable(
      @Nullable final String expression, @Nonnull final String message) {
    final UnprocessableEntityException exception = new UnprocessableEntityException(message);
    exception.setOperationOutcome(
        outcomeOf(List.of(issue(IssueType.INVALID, expression, message))));
    return exception;
  }

  /**
   * Translates a failure raised while planning or running a subject's query.
   *
   * <p>A Spark {@link AnalysisException} is a fault in the subject itself - an unresolved column,
   * an unknown function, a missing {@code GROUP BY}, an ambiguous reference - so it becomes a
   * {@code 422} carrying Spark's own analyser message, which names the problem and often suggests
   * the intended identifier. Analysis runs before execution, so that message cannot carry data
   * values.
   *
   * <p>Any other failure is a server-side fault and is returned unaltered, so it continues to
   * render as a {@code 500}. Runtime errors in particular are raised once the result is being
   * consumed, where the response may already be committed and the status can no longer be
   * rewritten.
   *
   * @param error the failure raised by the query engine
   * @return the exception to throw
   */
  @Nonnull
  public static RuntimeException executionFailure(@Nonnull final Exception error) {
    // AnalysisException is declared in Scala as a checked exception that no signature on the call
    // path declares, so it cannot be named in a catch clause and is matched by type instead.
    if (error instanceof final AnalysisException analysisError) {
      return unprocessable(
          SubjectResolver.SUBJECT_EXPRESSION,
          requireNonNullElse(analysisError.getMessage(), analysisError.toString()));
    }
    return error instanceof final RuntimeException runtimeError
        ? runtimeError
        : new RuntimeException(error);
  }

  /**
   * Builds an exception carrying several issues in one {@code OperationOutcome}, used where the
   * contract requires every problem in a request to be reported together. The status code decides
   * the exception type; the message is the concatenation of the issues' diagnostics.
   *
   * @param statusCode the HTTP status code to return
   * @param issues the issues to report, in order; must not be empty
   * @return the exception to throw
   * @throws IllegalArgumentException if {@code issues} is empty
   */
  @Nonnull
  public static BaseServerResponseException of(
      final int statusCode, @Nonnull final List<OperationOutcomeIssueComponent> issues) {
    if (issues.isEmpty()) {
      throw new IllegalArgumentException("At least one issue is required");
    }
    final String message =
        issues.stream()
            .map(OperationOutcomeIssueComponent::getDiagnostics)
            .reduce((a, b) -> a + "; " + b)
            .orElse("Invalid request");
    final BaseServerResponseException exception =
        BaseServerResponseException.newInstance(statusCode, message);
    exception.setOperationOutcome(outcomeOf(issues));
    return exception;
  }

  /**
   * Builds a single error-severity {@code OperationOutcome} issue.
   *
   * @param code the {@code issue.code} to report
   * @param expression the parameter at fault, or null when the failure names no parameter
   * @param message the diagnostics message
   * @return the issue component
   */
  @Nonnull
  public static OperationOutcomeIssueComponent issue(
      @Nonnull final IssueType code,
      @Nullable final String expression,
      @Nonnull final String message) {
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.ERROR);
    issue.setCode(code);
    issue.setDiagnostics(message);
    if (expression != null) {
      issue.addExpression(expression);
    }
    return issue;
  }

  @Nonnull
  private static OperationOutcome outcomeOf(
      @Nonnull final List<OperationOutcomeIssueComponent> issues) {
    final OperationOutcome outcome = new OperationOutcome();
    issues.forEach(outcome::addIssue);
    return outcome;
  }
}

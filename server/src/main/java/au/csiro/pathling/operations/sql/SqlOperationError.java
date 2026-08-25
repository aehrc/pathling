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
import org.apache.spark.sql.delta.DeltaAnalysisException;
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

  /** The longest analyser message returned to the caller, beyond which it is truncated. */
  private static final int MAX_ANALYSER_MESSAGE_LENGTH = 1024;

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
   * Translates a failure raised while planning a subject's query into a {@code 422 Unprocessable
   * Entity}, where it is a fault in the SQL the caller submitted rather than a server fault.
   *
   * <p>Spark's analyser is what catches an unresolved column, an unknown function, a missing {@code
   * GROUP BY} or an ambiguous reference, none of which {@code SqlValidator} can detect statically.
   * The response carries the analyser's own message: it names the problem and often suggests the
   * intended identifier. Analysis runs before execution, so that message cannot carry data values.
   *
   * <p>A Delta analysis failure is not translated. {@code DeltaAnalysisException} extends {@code
   * AnalysisException}, but it describes the state of the stored data rather than the request: a
   * schema mismatch between a stored table and this server's encoders, or a snapshot whose version
   * has been vacuumed away. Those are server-side faults, and {@code ErrorHandlingInterceptor}
   * already has a dedicated rendering for the schema-mismatch condition, which the raw Delta
   * message cannot be used for because it embeds both struct definitions in full.
   *
   * <p>Any other failure returns null and must be rethrown as it is, so that the unwrapping and
   * per-type conversions of {@code ErrorHandlingInterceptor} still see it. A runtime error raised
   * by the terminal consumer arrives that way, and stays a {@code 500}: it fires once the response
   * may already be committed, where the status can no longer be rewritten.
   *
   * @param subjectName the name of the subject at fault, or null where the request admits only one
   *     subject and there is nothing to disambiguate
   * @param error the failure raised by the query engine
   * @return the exception to throw, or null where the failure is not an analysis failure in the
   *     caller's own SQL
   */
  @Nullable
  public static UnprocessableEntityException asAnalysisFailure(
      @Nullable final String subjectName, @Nonnull final Exception error) {
    // AnalysisException is declared in Scala as a checked exception that no signature on the call
    // path declares, so it cannot be named in a catch clause and is matched by type instead.
    if (!(error instanceof final AnalysisException analysisError)
        || error instanceof DeltaAnalysisException) {
      return null;
    }
    final String message = analyserMessage(analysisError);
    return unprocessable(
        SubjectResolver.SUBJECT_EXPRESSION,
        subjectName == null
            ? message
            : "The subject '%s' cannot be processed: %s".formatted(subjectName, message));
  }

  /**
   * Renders an analyser failure for the wire. {@code getMessage} appends the whole unresolved
   * logical plan, which is unbounded, names internal request-scoped views and says nothing the
   * caller can act on; {@code getSimpleMessage} is the same condition, position and suggestions
   * without it. The result is bounded as well, since a suggestion list is drawn from the subject's
   * columns and a wide dependency makes it long.
   */
  @Nonnull
  private static String analyserMessage(@Nonnull final AnalysisException error) {
    @Nullable final String simpleMessage = error.getSimpleMessage();
    final String message =
        simpleMessage != null ? simpleMessage : requireNonNullElse(error.getMessage(), "");
    if (message.length() <= MAX_ANALYSER_MESSAGE_LENGTH) {
      return message;
    }
    // Step back off a trailing high surrogate, so that truncation never splits a character in two.
    final int end =
        Character.isHighSurrogate(message.charAt(MAX_ANALYSER_MESSAGE_LENGTH - 1))
            ? MAX_ANALYSER_MESSAGE_LENGTH - 1
            : MAX_ANALYSER_MESSAGE_LENGTH;
    return message.substring(0, end) + "...";
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

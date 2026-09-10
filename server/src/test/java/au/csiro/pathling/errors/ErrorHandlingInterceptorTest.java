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

package au.csiro.pathling.errors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.io.SchemaDriftError;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import jakarta.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import org.apache.spark.SparkException;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.sql.delta.DeltaAnalysisException;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.junit.jupiter.api.Test;
import scala.Option;

/**
 * Unit tests for {@link ErrorHandlingInterceptor}.
 *
 * @author John Grimes
 */
class ErrorHandlingInterceptorTest {

  // SchemaDriftError should map to a 500 response whose OperationOutcome diagnostics name the
  // affected resource type, the condition, and the remedies, rather than the generic
  // "Unexpected error occurred" message.
  @Test
  void convertsSchemaDriftErrorTo500WithActionableDiagnostics() {
    final SchemaDriftError e = new SchemaDriftError("ViewDefinition");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result.getStatusCode()).isEqualTo(500);
    assertThat(result.getOperationOutcome()).isInstanceOf(OperationOutcome.class);
    final OperationOutcome outcome = (OperationOutcome) result.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    final OperationOutcome.OperationOutcomeIssueComponent issue = outcome.getIssueFirstRep();
    assertThat(issue.getSeverity()).isEqualTo(OperationOutcome.IssueSeverity.ERROR);
    assertThat(issue.getCode()).isEqualTo(OperationOutcome.IssueType.PROCESSING);
    assertThat(issue.getDiagnostics())
        .contains("ViewDefinition")
        .contains("behind this server's encoders")
        .contains("schemaAutoMerge")
        .contains("update a resource of this type");
  }

  @Test
  void convertsUserRaisedExceptionTo400() {
    // SparkRuntimeException with USER_RAISED_EXCEPTION should return 400.
    final String errorMessage = "Expecting a collection with a single element but it has many.";
    final SparkRuntimeException e = mock(SparkRuntimeException.class);
    when(e.getCondition()).thenReturn("USER_RAISED_EXCEPTION");
    when(e.getMessage()).thenReturn(errorMessage);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
    assertThat(result.getMessage()).contains("Expecting a collection with a single element");
  }

  @Test
  void convertsOtherSparkRuntimeExceptionTo500() {
    // SparkRuntimeException with other error class should return 500.
    final SparkRuntimeException e = mock(SparkRuntimeException.class);
    when(e.getCondition()).thenReturn("SOME_OTHER_ERROR");
    when(e.getCause()).thenReturn(null);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InternalErrorException.class);
    assertThat(result.getStatusCode()).isEqualTo(500);
  }

  @Test
  void unwrapsSparkRuntimeExceptionWithCause() {
    // SparkRuntimeException with non-USER_RAISED_EXCEPTION should unwrap cause if present.
    final InvalidUserInputError cause = new InvalidUserInputError("Invalid input");
    final SparkRuntimeException e = mock(SparkRuntimeException.class);
    when(e.getCondition()).thenReturn("SOME_OTHER_ERROR");
    when(e.getCause()).thenReturn(cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
  }

  @Test
  void unwrapsSparkExceptionWithCause() {
    // SparkException wrapping a cause should unwrap to the cause.
    final InvalidUserInputError cause = new InvalidUserInputError("Input error");
    final SparkException e = new SparkException("wrapper", cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
  }

  @Test
  void sparkExceptionWithNoCauseReturns500() {
    // SparkException without a cause returns 500.
    final SparkException e = new SparkException("error");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InternalErrorException.class);
    assertThat(result.getStatusCode()).isEqualTo(500);
  }

  @Test
  void unwrapsUncheckedExecutionExceptionWithCause() {
    // UncheckedExecutionException wrapping a cause should unwrap.
    final InvalidUserInputError cause = new InvalidUserInputError("Cache error");
    final UncheckedExecutionException e = new UncheckedExecutionException(cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
  }

  @Test
  void unwrapsInvocationTargetExceptionWithCause() {
    // InvocationTargetException wrapping a cause should unwrap.
    final ResourceNotFoundError cause = new ResourceNotFoundError("not found");
    final InvocationTargetException e = new InvocationTargetException(cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(ResourceNotFoundException.class);
    assertThat(result.getStatusCode()).isEqualTo(404);
  }

  @Test
  void unwrapsUndeclaredThrowableExceptionWithCause() {
    // UndeclaredThrowableException wrapping a cause should unwrap.
    final InvalidUserInputError cause = new InvalidUserInputError("undeclared error");
    final UndeclaredThrowableException e = new UndeclaredThrowableException(cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
  }

  @Test
  void dataFormatExceptionWithNoCauseReturns400() {
    // DataFormatException without a cause returns 400 with FHIR error message.
    final DataFormatException e = new DataFormatException("Bad FHIR format");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
    assertThat(result.getMessage()).contains("Invalid FHIR content");
  }

  @Test
  void dataFormatExceptionWithJsonParseExceptionCause() {
    // DataFormatException wrapping JsonParseException returns 400 with JSON error.
    final JsonParseException cause = mock(JsonParseException.class);
    when(cause.getMessage()).thenReturn("Unexpected character");
    final DataFormatException e = new DataFormatException("Parse error", cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
    assertThat(result.getMessage()).contains("Invalid JSON content");
  }

  @Test
  void dataFormatExceptionWithOtherCause() {
    // DataFormatException wrapping other exception returns 400 with unknown error.
    final RuntimeException cause = new RuntimeException("Other parse issue");
    final DataFormatException e = new DataFormatException("Parse error", cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
    assertThat(result.getMessage()).contains("Unknown problem while parsing");
  }

  @Test
  void fhirClientConnectionExceptionReturns503() {
    // FhirClientConnectionException returns 503 Service Unavailable.
    final FhirClientConnectionException e = new FhirClientConnectionException("Connection failed");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result.getStatusCode()).isEqualTo(503);
  }

  @Test
  void resourceNotFoundErrorReturns404() {
    // ResourceNotFoundError returns 404.
    final ResourceNotFoundError e = new ResourceNotFoundError("Resource not found");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(ResourceNotFoundException.class);
    assertThat(result.getStatusCode()).isEqualTo(404);
  }

  @Test
  void accessDeniedErrorReturns403() {
    // AccessDeniedError returns 403 Forbidden.
    final AccessDeniedError e = new AccessDeniedError("Access denied");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result.getStatusCode()).isEqualTo(403);
    assertThat(result.getMessage()).contains("Access denied");
  }

  @Test
  void baseServerResponseExceptionPassesThrough() {
    // BaseServerResponseException with valid status code passes through.
    final InvalidRequestException e = new InvalidRequestException("Invalid request");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isSameAs(e);
  }

  @Test
  void baseServerResponseExceptionWithZeroStatusCodeReturns500() {
    // BaseServerResponseException with 0 status code returns 500.
    final BaseServerResponseException e = mock(BaseServerResponseException.class);
    when(e.getStatusCode()).thenReturn(0);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InternalErrorException.class);
    assertThat(result.getStatusCode()).isEqualTo(500);
  }

  @Test
  void unknownExceptionReturns500() {
    // Unknown exceptions return 500.
    final RuntimeException e = new RuntimeException("Unknown error");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InternalErrorException.class);
    assertThat(result.getStatusCode()).isEqualTo(500);
  }

  @Test
  void internalErrorExceptionWithCauseUnwraps() {
    // InternalErrorException wrapping a cause should unwrap.
    final InvalidUserInputError cause = new InvalidUserInputError("Inner error");
    final InternalErrorException e = new InternalErrorException("Outer", cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
  }

  // ---- Delta schema mismatch translation (FR-001, FR-002, FR-003) ----

  // A Delta schema mismatch where the source carries fields the table lacks must be translated into
  // an actionable message naming the missing field paths and the remedy for that direction, rather
  // than falling through to the generic "Unexpected error occurred" (FR-001).
  @Test
  void convertsDeltaSchemaMismatchWithMissingFieldsToActionableDiagnostics() {
    final Throwable e = schemaMismatch("struct<path:string,forEach:string>", "struct<path:string>");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result.getStatusCode()).isEqualTo(500);
    final String diagnostics = diagnosticsOf(result);
    assertThat(diagnostics)
        .contains("forEach")
        .contains("schemaAutoMerge")
        .doesNotContain("Unexpected error occurred");
  }

  // The resource type is not carried by the Delta exception, so it is supplied by the caller that
  // knows it. When supplied it must appear in the diagnostics (FR-001).
  @Test
  void namesTheResourceTypeWhenItIsKnown() {
    final Throwable e = schemaMismatch("struct<path:string,forEach:string>", "struct<path:string>");

    final BaseServerResponseException result =
        ErrorHandlingInterceptor.convertError(e, "ViewDefinition");

    assertThat(diagnosticsOf(result)).contains("ViewDefinition").contains("forEach");
  }

  // A Delta schema mismatch where the table carries fields the encoder does not emit must name
  // those paths and point at the encoding configuration rather than at schemaAutoMerge, because
  // that direction is not migratable (FR-001).
  @Test
  void convertsDeltaSchemaMismatchWithExcessFieldsToActionableDiagnostics() {
    final Throwable e =
        schemaMismatch("struct<url:string>", "struct<url:string,valuePeriod:string>");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    final String diagnostics = diagnosticsOf(result);
    assertThat(diagnostics)
        .contains("Patient")
        .contains("valuePeriod")
        .contains("openTypes")
        .doesNotContain("Unexpected error occurred");
  }

  // Where the two schemas differ in both directions at once, both sets of paths are reported and
  // the message distinguishes them.
  @Test
  void reportsBothDirectionsWhenTheyDifferAtOnce() {
    final Throwable e =
        schemaMismatch(
            "struct<url:string,forEach:string>", "struct<url:string,valuePeriod:string>");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    final String diagnostics = diagnosticsOf(result);
    assertThat(diagnostics).contains("forEach").contains("valuePeriod");
  }

  // The translated message must not leak the raw exception text, which embeds the full struct
  // definitions of both schemas, nor any warehouse path (FR-002, SC-003).
  @Test
  void translatedDiagnosticsExposeNoStructDefinitionOrWarehousePath() {
    final Throwable e = schemaMismatch("struct<path:string,forEach:string>", "struct<path:string>");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    final String diagnostics = diagnosticsOf(result);
    assertThat(diagnostics)
        .doesNotContain("struct<")
        .doesNotContain("Cannot cast")
        .doesNotContain("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION")
        .doesNotContain("file:/")
        .doesNotContain(".parquet");
  }

  // A Delta exception carrying the recognised condition but no usable field detail must still
  // produce a message naming the type and the condition, rather than the generic message.
  @Test
  void convertsDeltaSchemaMismatchWithoutFieldDetail() {
    final Throwable e = schemaMismatch("not a parseable type", "also not parseable");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    assertThat(result.getStatusCode()).isEqualTo(500);
    final String diagnostics = diagnosticsOf(result);
    assertThat(diagnostics)
        .contains("Patient")
        .contains("cannot be reconciled")
        .doesNotContain("Unexpected error occurred");
  }

  // The translation is on the error path, so it applies to an exception surfaced from a read as
  // readily as one from a write, including when Spark has wrapped it.
  @Test
  void translatesDeltaSchemaMismatchWrappedInSparkException() {
    final Throwable cause =
        schemaMismatch("struct<path:string,forEach:string>", "struct<path:string>");
    final SparkException e = new SparkException("Job aborted", cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    assertThat(diagnosticsOf(result)).contains("forEach").contains("Patient");
  }

  // A Delta exception carrying a different condition is not a schema mismatch, so it must continue
  // to yield the existing generic message with no internal detail added (FR-003).
  @Test
  void otherDeltaConditionsStillYieldTheGenericMessage() {
    final Throwable e =
        new DeltaAnalysisException(
            "DELTA_FAILED_TO_MERGE_FIELDS",
            new String[] {"a", "b"},
            Option.empty(),
            Option.empty());

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    assertThat(result).isInstanceOf(InternalErrorException.class);
    assertThat(result.getStatusCode()).isEqualTo(500);
    assertThat(result.getMessage()).isEqualTo("Unexpected error occurred");
  }

  // A struct can carry hundreds of fields, and a response body is not the place to enumerate them
  // all. The rendered list is bounded and the remainder summarised by a count, so a wide struct
  // cannot produce an unbounded message (FR-002).
  @Test
  void boundsTheNumberOfFieldPathsReported() {
    final StringBuilder wide = new StringBuilder("struct<kept:string");
    for (int i = 0; i < 25; i++) {
      wide.append(",field").append(i).append(":string");
    }
    final Throwable e = schemaMismatch(wide.append(">").toString(), "struct<kept:string>");

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e, "Patient");

    final String diagnostics = diagnosticsOf(result);
    // Ten paths are named and the remaining fifteen are counted, not listed. The names are reported
    // in lexicographic order, so field9 falls outside the reported ten.
    assertThat(diagnostics).contains("and 15 more").contains("field0").doesNotContain("field9");
  }

  /** Builds the Delta exception raised when a MERGE cannot cast the source struct to the target. */
  @Nonnull
  private static Throwable schemaMismatch(
      @Nonnull final String fromCatalog, @Nonnull final String toCatalog) {
    return new DeltaAnalysisException(
        "DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION",
        new String[] {fromCatalog, toCatalog},
        Option.empty(),
        Option.empty());
  }

  /** Extracts the single OperationOutcome issue's diagnostics from a converted exception. */
  @Nonnull
  private static String diagnosticsOf(@Nonnull final BaseServerResponseException result) {
    assertThat(result.getOperationOutcome()).isInstanceOf(OperationOutcome.class);
    final OperationOutcome outcome = (OperationOutcome) result.getOperationOutcome();
    assertThat(outcome.getIssue()).hasSize(1);
    return outcome.getIssueFirstRep().getDiagnostics();
  }
}

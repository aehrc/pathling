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

import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import org.apache.spark.SparkException;
import org.apache.spark.SparkRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ErrorHandlingInterceptor}.
 *
 * @author John Grimes
 */
class ErrorHandlingInterceptorTest {

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
}

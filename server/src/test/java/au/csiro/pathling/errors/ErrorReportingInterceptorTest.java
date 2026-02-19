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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ErrorReportingInterceptor} covering exception reportability checks, root cause
 * extraction, and the hook handler.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class ErrorReportingInterceptorTest {

  // -- isReportableException --

  @Test
  void serverErrorIsReportable() {
    // A 500 Internal Server Error should be reportable.
    final InternalErrorException exception = new InternalErrorException("Server error");
    assertTrue(ErrorReportingInterceptor.isReportableException(exception));
  }

  @Test
  void serviceUnavailableIsReportable() {
    // A 503 Service Unavailable should be reportable.
    final UnclassifiedServerFailureException exception =
        new UnclassifiedServerFailureException(503, "Unavailable");
    assertTrue(ErrorReportingInterceptor.isReportableException(exception));
  }

  @Test
  void badRequestIsNotReportable() {
    // A 400 Bad Request should not be reportable.
    final InvalidRequestException exception = new InvalidRequestException("Bad input");
    assertFalse(ErrorReportingInterceptor.isReportableException(exception));
  }

  @Test
  void notFoundIsNotReportable() {
    // A 404 Not Found should not be reportable.
    final ResourceNotFoundException exception = new ResourceNotFoundException("Not found");
    assertFalse(ErrorReportingInterceptor.isReportableException(exception));
  }

  // -- getReportableError --

  @Test
  void getReportableErrorReturnsExceptionWhenNoCause() {
    // When the exception has no cause, it should be returned as-is.
    final InternalErrorException exception = new InternalErrorException("Server error");
    final Throwable result = ErrorReportingInterceptor.getReportableError(exception);
    assertSame(exception, result);
  }

  @Test
  void getReportableErrorReturnsCauseWhenPresent() {
    // When the exception has a cause, the cause should be returned.
    final RuntimeException cause = new RuntimeException("root cause");
    final InternalErrorException exception = new InternalErrorException("Server error", cause);
    final Throwable result = ErrorReportingInterceptor.getReportableError(exception);
    assertSame(cause, result);
  }

  // -- reportErrorToSentry (hook method) --

  @Test
  void reportErrorToSentryHandlesNullException() {
    // The hook method should not throw when exception is null.
    final ErrorReportingInterceptor interceptor = new ErrorReportingInterceptor();
    interceptor.reportErrorToSentry(null, null, null, null, null);
    // No exception means the null check works correctly.
  }

  @Test
  void reportErrorToSentryHandlesNonReportableException() {
    // The hook method should not report 4xx errors.
    final ErrorReportingInterceptor interceptor = new ErrorReportingInterceptor();
    final InvalidRequestException exception = new InvalidRequestException("Bad input");
    // This should not throw - non-reportable exceptions are silently ignored.
    interceptor.reportErrorToSentry(null, null, null, null, exception);
  }
}

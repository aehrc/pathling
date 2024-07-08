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

package au.csiro.pathling.test.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.ForbiddenOperationException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import jakarta.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import org.apache.spark.SparkException;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test error handling.
 *
 * @author Piotr Szul
 */
@Tag("UnitTest")
class ErrorHandlingInterceptorTest {

  final static BaseServerResponseException SERVER_RESPONSE_EX1 = new NotModifiedException(
      "NotModifiedException");
  final static BaseServerResponseException SERVER_RESPONSE_EX2 = new ForbiddenOperationException(
      "ForbiddenOperationException");

  @Nonnull
  final ErrorHandlingInterceptor errorHandlingInterceptor = new ErrorHandlingInterceptor();

  @Nonnull
  BaseServerResponseException callInterceptor(final Throwable error) {
    return errorHandlingInterceptor.handleOutgoingException(null, null, error, null, null);
  }

  @Test
  void passesThruBaseServerResponseExceptions() {
    assertEquals(SERVER_RESPONSE_EX1, callInterceptor(SERVER_RESPONSE_EX1));
    assertEquals(SERVER_RESPONSE_EX2, callInterceptor(SERVER_RESPONSE_EX2));
  }

  @Test
  void passesThruRootInternalErrorException() {
    final Exception rootException = new InternalErrorException("RootException");
    assertEquals(rootException, callInterceptor(rootException));
  }

  @Test
  void recursiveUnwrappingOfException() {
    final Exception wrappedException = new UndeclaredThrowableException(
        new InternalErrorException(new InvocationTargetException(
            new SparkException("Spark Error",
                new UncheckedExecutionException(SERVER_RESPONSE_EX1)))));
    assertEquals(SERVER_RESPONSE_EX1, callInterceptor(wrappedException));
  }

  @Test
  void wrapsUnknownExceptions() {
    final BaseServerResponseException actualException = callInterceptor(
        new RuntimeException("RuntimeException"));
    assertTrue(actualException instanceof InternalErrorException);
  }

  @Test
  @SuppressWarnings({
      "serial"})
  void wrapsBaseServerResponseExceptionsWithZeroStatus() {
    final BaseServerResponseException actualException = callInterceptor(
        new BaseServerResponseException(0, "IllegalStatus") {
        });
    assertTrue(actualException instanceof InternalErrorException);
  }

  @Test
  void convertsFhirClientException() {
    final BaseServerResponseException actualException = callInterceptor(
        new FhirClientConnectionException("FhirClientConnectionException message")
    );
    assertTrue(actualException instanceof UnclassifiedServerFailureException);
    assertEquals(503, actualException.getStatusCode());
    assertEquals("FhirClientConnectionException message", actualException.getMessage());
  }

  @Test
  void convertsDataFormatExceptionsWithJsonError() {
    final BaseServerResponseException actualException = callInterceptor(
        new DataFormatException(new JsonParseException(null,
            "Unexpected character ('{' (code 123)): was expecting double-quote to"
                + " start field name\\n at [Source: UNKNOWN; line: 1, column: 3]")));
    assertTrue(actualException instanceof InvalidRequestException);
    assertEquals("Invalid JSON content: "
            + "Unexpected character ('{' (code 123)): was expecting double-quote to"
            + " start field name\\n at [Source: UNKNOWN; line: 1, column: 3]",
        actualException.getMessage());
  }

  @Test
  void convertsDataFormatExceptionsWithFhirError() {
    final BaseServerResponseException actualException = callInterceptor(
        new DataFormatException(
            "Invalid JSON content detected, missing required element: 'resourceType'"));
    assertTrue(actualException instanceof InvalidRequestException);
    assertEquals("Invalid FHIR content: "
            + "Invalid JSON content detected, missing required element: 'resourceType'",
        actualException.getMessage());
  }

  @Test
  void convertsDataFormatExceptionsWithUnknownError() {
    final BaseServerResponseException actualException = callInterceptor(
        new DataFormatException(new RuntimeException("Some Error")));
    assertTrue(actualException instanceof InvalidRequestException);
    assertEquals("Unknown problem while parsing FHIR/JSON content: "
            + "Some Error",
        actualException.getMessage());
  }

  @Test
  void convertsAccessDeniedError() {
    final BaseServerResponseException actualException = callInterceptor(
        new AccessDeniedError("Access denied", "operation:import")
    );
    assertTrue(actualException instanceof ForbiddenOperationException);
    assertEquals(403, actualException.getStatusCode());
    assertEquals("Access denied", actualException.getMessage());

    final OperationOutcomeIssueComponent expectedIssue = new OperationOutcomeIssueComponent();
    expectedIssue.setSeverity(IssueSeverity.ERROR);
    expectedIssue.setCode(IssueType.FORBIDDEN);
    expectedIssue.setDiagnostics("Access denied");

    assertTrue(expectedIssue
        .equalsDeep(((OperationOutcome) actualException.getOperationOutcome()).getIssueFirstRep())
    );
  }

}

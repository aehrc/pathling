/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhir.ErrorHandlingInterceptor;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.*;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nonnull;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test error handling.
 *
 * @author Piotr Szul
 */
@Tag("UnitTest")
public class ErrorHandlingInterceptorTest {

  private final static BaseServerResponseException SERVER_RESPONSE_EX1 = new NotModifiedException(
      "NotModifiedException");
  private final static BaseServerResponseException SERVER_RESPONSE_EX2 = new ForbiddenOperationException(
      "ForbiddenOperationException");

  @Nonnull
  private final ErrorHandlingInterceptor errorHandlingInterceptor = new ErrorHandlingInterceptor();

  @Nonnull
  private BaseServerResponseException callInterceptor(final Throwable error) {
    return errorHandlingInterceptor.convertError(null, null, error, null, null);
  }

  @Test
  public void passesThruBaseServerResponseExceptions() {
    assertEquals(SERVER_RESPONSE_EX1, callInterceptor(SERVER_RESPONSE_EX1));
    assertEquals(SERVER_RESPONSE_EX2, callInterceptor(SERVER_RESPONSE_EX2));
  }

  @Test
  public void passesThruRootInternalErrorException() {
    final Exception rootException = new InternalErrorException("RootException");
    assertEquals(rootException, callInterceptor(rootException));
  }

  @Test
  public void recursiveUnwrappingOfException() {
    final Exception wrappedException = new InternalErrorException(new InvocationTargetException(
        new SparkException("Spark Error", new UncheckedExecutionException(SERVER_RESPONSE_EX1))));
    assertEquals(SERVER_RESPONSE_EX1, callInterceptor(wrappedException));
  }

  @Test
  public void wrapsUnknownExceptions() {
    final BaseServerResponseException actualException = callInterceptor(
        new RuntimeException("RuntimeException"));
    assertTrue(actualException instanceof InternalErrorException);
  }

  @Test
  @SuppressWarnings({"SerializableNonStaticInnerClassWithoutSerialVersionUID",
      "SerializableInnerClassWithNonSerializableOuterClass"})
  public void wrapsBaseServerResponseExceptionsWithZeroStatus() {
    final BaseServerResponseException actualException = callInterceptor(
        new BaseServerResponseException(0, "IllegalStatus") {
        });
    assertTrue(actualException instanceof InternalErrorException);
  }

  @Test
  public void convertsFhirClientException() {
    final BaseServerResponseException actualException = callInterceptor(
        new FhirClientConnectionException("FhirClientConnectionException message")
    );
    assertTrue(actualException instanceof UnclassifiedServerFailureException);
    assertEquals(503, actualException.getStatusCode());
    assertEquals("FhirClientConnectionException message", actualException.getMessage());
  }

  @Test
  public void convertsDataFormatExceptionsWithJsonError() {
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
  public void convertsDataFormatExceptionsWithFhirError() {
    final BaseServerResponseException actualException = callInterceptor(
        new DataFormatException(
            "Invalid JSON content detected, missing required element: 'resourceType'"));
    assertTrue(actualException instanceof InvalidRequestException);
    assertEquals("Invalid FHIR content: "
            + "Invalid JSON content detected, missing required element: 'resourceType'",
        actualException.getMessage());
  }

  @Test
  public void convertsDataFormatExceptionsWithUnknownError() {
    final BaseServerResponseException actualException = callInterceptor(
        new DataFormatException(new RuntimeException("Some Error")));
    assertTrue(actualException instanceof InvalidRequestException);
    assertEquals("Unknown problem while parsing FHIR/JSON content: "
            + "Some Error",
        actualException.getMessage());
  }

}

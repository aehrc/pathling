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

package au.csiro.pathling.errors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
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
    when(e.getErrorClass()).thenReturn("USER_RAISED_EXCEPTION");
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
    when(e.getErrorClass()).thenReturn("SOME_OTHER_ERROR");
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
    when(e.getErrorClass()).thenReturn("SOME_OTHER_ERROR");
    when(e.getCause()).thenReturn(cause);

    final BaseServerResponseException result = ErrorHandlingInterceptor.convertError(e);

    assertThat(result).isInstanceOf(InvalidRequestException.class);
    assertThat(result.getStatusCode()).isEqualTo(400);
  }
}

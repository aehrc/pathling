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

import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import jakarta.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.spark.SparkException;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * This class unifies exception handling.
 *
 * @author Piotr Szul
 */
@Interceptor
public class ErrorHandlingInterceptor {

  /**
   * HAPI hook to convert errors and exceptions to BaseServerResponseException
   *
   * @param requestDetails the details of the request (HAPI)
   * @param servletRequestDetails further details of the request (HAPI)
   * @param throwable the exception to process
   * @param request the details of the request (Servlet API)
   * @param response the response that will be sent
   * @return an exception of type {@link BaseServerResponseException}
   */
  @Hook(Pointcut.SERVER_PRE_PROCESS_OUTGOING_EXCEPTION)
  @SuppressWarnings("unused")
  public BaseServerResponseException handleOutgoingException(
      @Nullable final RequestDetails requestDetails,
      @Nullable final ServletRequestDetails servletRequestDetails,
      @Nonnull final Throwable throwable,
      @Nullable final HttpServletRequest request, @Nullable final HttpServletResponse response) {

    return convertError(throwable);
  }

  /**
   * @param error an error that could be raised during processing
   * @return a HAPI {@link BaseServerResponseException} that will deliver an appropriate response to
   * a user of the FHIR API
   */
  @Nonnull
  public static BaseServerResponseException convertError(@Nonnull final Throwable error) {
    try {
      throw error;

    } catch (final SparkException | UncheckedExecutionException | InternalErrorException |
                   InvocationTargetException | UndeclaredThrowableException e) {
      // A number of exceptions are being used to wrap the actual cause. In this case we unwrap
      // its cause and pass it back to this same method to be re-evaluated.
      //
      // A SparkException is thrown when an error occurs inside a Spark job.
      //
      // InvocationTargetException wrapped in InternalErrorException is thrown when a non
      // BaseServerResponseException is thrown from a IResourceProvider
      // (see: ca.uhn.fhir.rest.server.method.BaseMethodBinding.invokeServerMethod )
      @Nullable final Throwable cause = e.getCause();
      if (cause != null) {
        return convertError(cause);
      } else {
        return internalServerError(e);
      }
    } catch (final DataFormatException e) {
      return convertDataFormatException(e);

    } catch (final FhirClientConnectionException e) {
      // Special case for FhirClientConnectionException
      // return error 503 as per issue #146
      return BaseServerResponseException.newInstance(SERVICE_UNAVAILABLE.value(), e.getMessage());

    } catch (final BaseServerResponseException e) {
      // We pass HAPI exceptions through unaltered unless they do not include a valid HTTP
      // status code.
      if (e.getStatusCode() == 0) {
        return internalServerError(e);
      } else {
        return e;
      }

    } catch (final ResourceNotFoundError e) {
      // Errors relating to resources not found are passed through using the corresponding HAPI
      // exception.
      return new ResourceNotFoundException(e.getMessage());

    } catch (final InvalidUserInputError e) {
      // Errors relating to invalid user input are passed through using the corresponding HAPI
      // exception.
      return new InvalidRequestException(e);
    } catch (final AccessDeniedError e) {
      return buildException(HttpServletResponse.SC_FORBIDDEN, e.getMessage(), IssueType.FORBIDDEN);
    } catch (final Throwable e) {
      // Anything else is unexpected and triggers a 500.
      return internalServerError(e);
    }
  }

  @Nonnull
  @SuppressWarnings("SameParameterValue")
  private static BaseServerResponseException buildException(final int theStatusCode,
      @Nonnull final String message,
      @Nonnull final IssueType issueType) {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.ERROR);
    issue.setDiagnostics(message);
    issue.setCode(issueType);
    opOutcome.addIssue(issue);
    final BaseServerResponseException ex = BaseServerResponseException
        .newInstance(theStatusCode, message);
    ex.setOperationOutcome(opOutcome);
    return ex;
  }


  @Nonnull
  private static BaseServerResponseException convertDataFormatException(
      @Nonnull final DataFormatException e) {
    final Throwable cause = e.getCause();
    if (cause == null) {
      // A problem with constructing FHIR from JSON.
      return new InvalidRequestException("Invalid FHIR content: " + e.getMessage());
    } else {
      if (cause instanceof JsonParseException) {
        // A problem with parsing JSON.
        return new InvalidRequestException("Invalid JSON content: " + cause.getMessage());
      } else {
        // An unknown problem with FHIR/JSON content.
        return new InvalidRequestException(
            "Unknown problem while parsing FHIR/JSON content: " + cause.getMessage());
      }
    }
  }

  @Nonnull
  private static InternalErrorException internalServerError(final @Nonnull Throwable error) {
    return error instanceof InternalErrorException
           ? (InternalErrorException) error
           : new InternalErrorException("Unexpected error occurred", error);
  }

}

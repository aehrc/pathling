/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.spark.SparkException;

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
  public BaseServerResponseException convertError(@Nullable final RequestDetails requestDetails,
      @Nullable final ServletRequestDetails servletRequestDetails,
      @Nonnull final Throwable throwable,
      @Nullable final HttpServletRequest request, @Nullable final HttpServletResponse response) {

    return doConvertError(throwable);
  }

  @Nonnull
  private BaseServerResponseException doConvertError(@Nonnull final Throwable error) {
    try {
      throw error;

    } catch (final SparkException | UncheckedExecutionException | InternalErrorException | InvocationTargetException e) {
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
        return doConvertError(cause);
      } else {
        return internalServerError(e);
      }

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

    } catch (final Throwable e) {
      // Anything else is unexpected and triggers a 500.
      return internalServerError(e);
    }

  }

  @Nonnull
  private InternalErrorException internalServerError(final @Nonnull Throwable error) {
    return error instanceof InternalErrorException
           ? (InternalErrorException) error
           : new InternalErrorException("Unexpected error occurred", error);
  }

}

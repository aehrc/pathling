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

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import io.sentry.Sentry;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class intercepts errors and reports them to Sentry.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Interceptor
@Slf4j
public class ErrorReportingInterceptor {

  /**
   * HAPI hook to intercept errors and report them to Sentry.
   *
   * @param requestDetails the details of the request (HAPI)
   * @param servletRequestDetails further details of the request (HAPI)
   * @param request the details of the request (Servlet API)
   * @param response the response that will be sent
   * @param exception the exception that was caught
   */
  @Hook(Pointcut.SERVER_HANDLE_EXCEPTION)
  @SuppressWarnings("unused")
  public void reportErrorToSentry(@Nullable final RequestDetails requestDetails,
      @Nullable final ServletRequestDetails servletRequestDetails,
      @Nullable final HttpServletRequest request, @Nullable final HttpServletResponse response,
      @Nullable final BaseServerResponseException exception) {

    if (exception != null) {
      reportExceptionToSentry(exception);
    }
  }

  /**
   * Checks if the exception constitutes an unexpected error (as opposed to a user input error) that
   * should be reported for investigation (e.g. to Sentry). We only want to report 500 series
   * errors, not errors such as resource not found and bad request.
   *
   * @param exception the exception to test
   * @return true if the exception should be reported, false otherwise
   */
  public static boolean isReportableException(
      @Nonnull final BaseServerResponseException exception) {
    return exception.getStatusCode() / 100 == 5;
  }

  /**
   * Gets the root cause exception from the server BaseServerResponseException.
   *
   * @param exception the exception to extracts the root cause from.
   * @return the root cause of this exception.
   */
  @Nonnull
  public static Throwable getReportableError(@Nonnull final BaseServerResponseException exception) {
    return exception.getCause() == null
           ? exception
           : exception.getCause();
  }

  /**
   * Reports the exception to Sentry if reportable.
   *
   * @param exception the exception to be reported.
   */
  public static void reportExceptionToSentry(
      @Nonnull final BaseServerResponseException exception) {
    if (isReportableException(exception)) {
      Sentry.captureException(getReportableError(exception));
    }
  }
}

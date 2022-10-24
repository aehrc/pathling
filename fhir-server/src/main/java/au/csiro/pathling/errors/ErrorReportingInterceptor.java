/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import io.sentry.Sentry;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
    // We only want to report 500 series errors, not errors such as resource not found and bad
    // request.
    final boolean errorReportable = exception != null && exception.getStatusCode() / 100 == 5;
    if (errorReportable) {
      final Throwable reportableError = exception.getCause() == null
                                        ? exception
                                        : exception.getCause();
      Sentry.captureException(reportableError);
    }
  }

}

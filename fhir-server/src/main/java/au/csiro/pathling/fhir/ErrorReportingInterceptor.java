/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import io.sentry.Sentry;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class intercepts errors and reports them to Sentry.
 *
 * @author John Grimes
 */
public class ErrorReportingInterceptor {

  private final String serverVersion;

  public ErrorReportingInterceptor(String serverVersion) {
    this.serverVersion = serverVersion;
  }

  @Hook(Pointcut.SERVER_HANDLE_EXCEPTION)
  public void reportErrorToSentry(RequestDetails requestDetails,
      ServletRequestDetails servletRequestDetails, HttpServletRequest request,
      HttpServletResponse response, BaseServerResponseException exception) {
    if (serverVersion != null) {
      Sentry.getContext().addExtra("serverVersion", serverVersion);
    }
    Sentry.capture(exception.getCause() == null
        ? exception
        : exception.getCause());
  }

}

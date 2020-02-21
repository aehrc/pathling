/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import io.sentry.Sentry;
import org.slf4j.MDC;

/**
 * This class adds the HAPI request ID to the logging context.
 *
 * @author John Grimes
 */
@Interceptor
public class RequestIdInterceptor {

  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
  public void addRequestIdToLoggingContext(RequestDetails requestDetails) {
    String requestId = requestDetails.getRequestId();
    MDC.put("requestId", requestId);
    Sentry.getContext().addExtra("requestId", requestId);
  }

}

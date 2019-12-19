/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
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
    MDC.put("requestId", requestDetails.getRequestId());
  }

}

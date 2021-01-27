/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import io.sentry.Sentry;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * This class adds the HAPI request ID to the logging context.
 *
 * @author John Grimes
 */
@Component
@Interceptor
@Slf4j
public class RequestIdInterceptor {

  /**
   * HAPI hook to intercept all requests and add a request ID to the logging context.
   *
   * @param requestDetails the details of the request
   */
  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
  @SuppressWarnings("unused")
  public void addRequestIdToLoggingContext(@Nullable final RequestDetails requestDetails) {
    if (requestDetails != null) {
      final String requestId = requestDetails.getRequestId();
      MDC.put("requestId", requestId);
      Sentry.configureScope(scope -> scope.setExtra("requestId", requestId));
    } else {
      log.warn("Request ID interceptor invoked with missing request details");
    }
  }

}

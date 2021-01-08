/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.Configuration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import io.sentry.Sentry;
import io.sentry.SentryEvent;
import io.sentry.protocol.Request;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * This class intercepts errors and reports them to Sentry.
 *
 * @author John Grimes
 */
@Component
@Interceptor
@Slf4j
public class ErrorReportingInterceptor {

  @Nonnull
  private final String serverVersion;

  @Nonnull
  private final IParser jsonParser;

  private ErrorReportingInterceptor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext) {
    jsonParser = fhirContext.newJsonParser();
    this.serverVersion = configuration.getVersion();
  }

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
    Sentry.setExtra("serverVersion", serverVersion);
    // We only want to report 500 series errors, not errors such as resource not found and bad
    // request.
    final boolean errorReportable = exception != null && exception.getStatusCode() / 100 == 5;
    if (errorReportable) {
      final Throwable reportableError = exception.getCause() == null
                                        ? exception
                                        : exception.getCause();
      if (servletRequestDetails != null && request != null) {
        final SentryEvent event = new SentryEvent();
        event.setRequest(buildSentryRequest(servletRequestDetails, request));
        Sentry.captureEvent(event);
      } else {
        log.warn("Failed to capture HTTP request details for error");
      }
      Sentry.captureException(reportableError);
    }
  }

  @Nonnull
  private Request buildSentryRequest(@Nonnull final ServletRequestDetails servletRequestDetails,
      @Nonnull final HttpServletRequest request) {
    final Request sentryRequest = new Request();

    // Harvest details out of the servlet request, and HAPI's request object. The reason we need to
    // do this is that unfortunately HAPI clears the body of the request out of the
    // HttpServletRequest object, so we need to use the more verbose constructor. The other reason
    // is that we want to omit any identifying details of users, such as remoteAddr, for privacy
    // purposes.
    sentryRequest.setUrl(servletRequestDetails.getCompleteUrl());
    sentryRequest.setMethod(request.getMethod());
    sentryRequest.setQueryString(request.getQueryString());
    final Map<String, String> sentryHeaders = new HashMap<>();
    for (final String key : servletRequestDetails.getHeaders().keySet()) {
      sentryHeaders.put(key, String.join(",", servletRequestDetails.getHeaders().get(key)));
    }
    sentryRequest.setHeaders(sentryHeaders);
    if (servletRequestDetails.getResource() != null) {
      sentryRequest.setData(jsonParser.encodeResourceToString(servletRequestDetails.getResource()));
    }

    return sentryRequest;
  }

}

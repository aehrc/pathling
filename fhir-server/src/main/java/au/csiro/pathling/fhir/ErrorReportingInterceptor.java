/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
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
import io.sentry.event.interfaces.HttpInterface;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
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
    Sentry.getContext().addExtra("serverVersion", serverVersion);
    // We only want to report 500 series errors, not errors such as resource not found and bad
    // request.
    final boolean errorReportable = exception != null && exception.getStatusCode() / 100 == 5;
    if (errorReportable) {
      final Throwable reportableError = exception.getCause() == null
                                        ? exception
                                        : exception.getCause();
      if (servletRequestDetails != null && request != null) {
        final HttpInterface http = buildHttpInterface(servletRequestDetails, request);
        Sentry.getContext().setHttp(http);
      } else {
        log.warn("Failed to capture HTTP request details for error");
      }
      Sentry.capture(reportableError);
    }
  }

  @Nonnull
  private HttpInterface buildHttpInterface(
      @Nonnull final ServletRequestDetails servletRequestDetails,
      @Nonnull final HttpServletRequest request) {
    // Harvest details out of the servlet request, and HAPI's request object. The reason we need to
    // do this is that unfortunately HAPI clears the body of the request out of the
    // HttpServletRequest object, so we need to use the more verbose constructor. The other reason
    // is that we want to omit any identifying details of users, such as remoteAddr, for privacy
    // purposes.
    final String requestUrl = servletRequestDetails.getCompleteUrl();
    final String method = request.getMethod();
    @SuppressWarnings("unchecked")
    final Map<String, Collection<String>> parameters = (Map<String, Collection<String>>) (Object)
        servletRequestDetails.getParameters().entrySet().stream()
            .collect(Collectors.toMap(
                Entry::getKey,
                e -> Arrays.asList(e.getValue())
            ));
    final String queryString = request.getQueryString();
    final Map<String, String> cookies = new HashMap<>();
    final String serverName = request.getServerName();
    final int serverPort = request.getServerPort();
    final String localAddr = request.getLocalAddr();
    final String localName = request.getLocalName();
    final int localPort = request.getLocalPort();
    final String protocol = request.getProtocol();
    final boolean secure = request.isSecure();
    final boolean asyncStarted = request.isAsyncStarted();
    final String authType = request.getAuthType();
    // This horrible casting hack (and the one in the assignment to parameters above) is here to
    // get around a problem in Java which prevents us from assigning the Map with List values to
    // a variable typed as a Map with Collection values.
    @SuppressWarnings("unchecked") final Map<String, Collection<String>> headers =
        (Map<String, Collection<String>>) (Object) servletRequestDetails.getHeaders();
    String body = null;
    if (servletRequestDetails.getResource() != null) {
      body = jsonParser.encodeResourceToString(servletRequestDetails.getResource());
    }

    return new HttpInterface(requestUrl, method, parameters, queryString,
        cookies, null, serverName, serverPort, localAddr, localName, localPort, protocol,
        secure, asyncStarted, authType, null, headers, body);
  }

}

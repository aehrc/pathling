/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import io.sentry.Sentry;
import io.sentry.event.interfaces.HttpInterface;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class intercepts errors and reports them to Sentry.
 *
 * @author John Grimes
 */
public class ErrorReportingInterceptor {

  private final String serverVersion;
  private final IParser jsonParser;

  public ErrorReportingInterceptor(FhirContext fhirContext, String serverVersion) {
    jsonParser = fhirContext.newJsonParser();
    this.serverVersion = serverVersion;
  }

  @Hook(Pointcut.SERVER_HANDLE_EXCEPTION)
  public void reportErrorToSentry(RequestDetails requestDetails,
      ServletRequestDetails servletRequestDetails, HttpServletRequest request,
      HttpServletResponse response, BaseServerResponseException exception) throws IOException {
    if (serverVersion != null) {
      Sentry.getContext().addExtra("serverVersion", serverVersion);
    }
    // We only want to report 500 series errors, not errors such as resource not found and bad
    // request.
    boolean errorReportable = exception != null && exception.getStatusCode() / 100 == 5;
    if (errorReportable) {
      Throwable reportableError = exception.getCause() == null
                                  ? exception
                                  : exception.getCause();
      HttpInterface http = buildHttpInterface(servletRequestDetails, request);
      Sentry.getContext().setHttp(http);
      Sentry.capture(reportableError);
    }
  }

  private HttpInterface buildHttpInterface(ServletRequestDetails servletRequestDetails,
      HttpServletRequest request) {
    // Harvest details out of the servlet request, and HAPI's request object. The reason we need to
    // do this is that unfortunately HAPI clears the body of the request out of the
    // HttpServletRequest object, so we need to use the more verbose constructor. The other reason
    // is that we want to omit any identifying details of users, such as remoteAddr, for privacy
    // purposes.
    String requestUrl = servletRequestDetails.getCompleteUrl();
    String method = request.getMethod();
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> parameters = (Map<String, Collection<String>>) (Object) servletRequestDetails
        .getParameters().entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> Arrays.asList(e.getValue())
        ));
    String queryString = request.getQueryString();
    Map<String, String> cookies = new HashMap<>();
    String serverName = request.getServerName();
    int serverPort = request.getServerPort();
    String localAddr = request.getLocalAddr();
    String localName = request.getLocalName();
    int localPort = request.getLocalPort();
    String protocol = request.getProtocol();
    boolean secure = request.isSecure();
    boolean asyncStarted = request.isAsyncStarted();
    String authType = request.getAuthType();
    // This horrible casting hack (and the one in the assignment to parameters above) is here to
    // get around a problem in Java which prevents us from assigning the Map with List values to
    // a variable typed as a Map with Collection values.
    @SuppressWarnings("unchecked")
    Map<String, Collection<String>> headers = (Map<String, Collection<String>>) (Object) servletRequestDetails
        .getHeaders();
    String body = null;
    if (servletRequestDetails.getResource() != null) {
      body = jsonParser.encodeResourceToString(servletRequestDetails.getResource());
    }

    return new HttpInterface(requestUrl, method, parameters, queryString,
        cookies, null, serverName, serverPort, localAddr, localName, localPort, protocol,
        secure, asyncStarted, authType, null, headers, body);
  }

}

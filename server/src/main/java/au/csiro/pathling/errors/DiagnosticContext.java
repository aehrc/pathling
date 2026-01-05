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

import static java.util.Objects.requireNonNull;

import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import io.sentry.Sentry;
import io.sentry.protocol.Request;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.MDC;

/**
 * This class represents the diagnostic information collected from a servlet request for the purpose
 * of logging and Sentry reporting.
 *
 * <p>The data in this class are not bound to the scope of the current request and can be used to
 * set diagnostic information even after the original request has been completed (e.g. for the
 * asynchronous worker threads).
 *
 * @author Piotr Szul
 */
public class DiagnosticContext {

  public static final String REQUEST_ID_TAG = "request-id";
  public static final String REQUEST_MODE_TAG = "request-mode";
  public static final String REQUEST_MODE_SYNC = "sync";
  public static final String REQUEST_MODE_ASYNC = "async";

  @Nullable private String requestId;

  @Nullable private Request request;

  private DiagnosticContext() {}

  private DiagnosticContext(@Nonnull final String requestId, @Nonnull final Request request) {
    this.requestId = requestId;
    this.request = request;
  }

  /**
   * Collects the diagnostic information from the given request details.
   *
   * @param servletRequestDetails the details of the current servlet request
   * @param jsonParser the jsonParse to use to serialize resource passed in the request (if present)
   * @return the diagnostic information.
   */
  public static DiagnosticContext fromRequest(
      @Nonnull final ServletRequestDetails servletRequestDetails,
      @Nonnull final IParser jsonParser) {

    return new DiagnosticContext(
        servletRequestDetails.getRequestId(),
        buildSentryRequest(servletRequestDetails, jsonParser));
  }

  /**
   * Creates an empty context for diagnostic information.
   *
   * @return the empty diagnostic information.
   */
  public static DiagnosticContext empty() {
    return new DiagnosticContext();
  }

  /**
   * Retrieves diagnostic information from the current Sentry scope.
   *
   * @return the diagnostic information from the current Sentry scope.
   */
  public static DiagnosticContext fromSentryScope() {
    final DiagnosticContext context = new DiagnosticContext();
    Sentry.withScope(
        scope -> {
          //noinspection UnstableApiUsage
          context.requestId = scope.getTags().get(REQUEST_ID_TAG);
          context.request = scope.getRequest();
        });
    return context;
  }

  /**
   * Configures the current thread's logging and Sentry contexts with the diagnostic information.
   *
   * @param asynchronous true if the current thread the worker for an asynchronous request.
   */
  public void configureScope(final boolean asynchronous) {
    MDC.put("requestId", requestId);
    Sentry.configureScope(
        scope -> {
          if (requestId != null) {
            scope.setTag(REQUEST_ID_TAG, requireNonNull(requestId));
          } else {
            scope.removeTag(REQUEST_ID_TAG);
          }
          scope.setTag(REQUEST_MODE_TAG, asynchronous ? REQUEST_MODE_ASYNC : REQUEST_MODE_SYNC);
          scope.setRequest(request);
        });
  }

  /**
   * Configures the current thread's logging and Sentry contexts with the diagnostic information for
   * synchronous requests.
   */
  public void configureScope() {
    configureScope(false);
  }

  @Nonnull
  private static Request buildSentryRequest(
      @Nonnull final ServletRequestDetails servletRequestDetails,
      @Nonnull final IParser jsonParser) {

    final HttpServletRequest request = servletRequestDetails.getServletRequest();
    final Request sentryRequest = new Request();

    // Harvest details out of the servlet request, and the HAPI request object. The reason we need
    // to do this is that unfortunately HAPI clears the body of the request out of the
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

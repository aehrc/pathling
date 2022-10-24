/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class adds diagnostic information about the current request to logging and Sentry contexts.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Interceptor
@Slf4j
public class DiagnosticContextInterceptor {

  @Nonnull
  private final IParser jsonParser;

  private DiagnosticContextInterceptor(@Nonnull final FhirContext fhirContext) {
    jsonParser = fhirContext.newJsonParser();
  }

  /**
   * HAPI hook to intercept all requests and adds diagnostic information related to the current
   * request to logging and Sentry contexts.
   *
   * @param servletRequestDetails the details of the current servlet request
   */
  @Hook(value = Pointcut.SERVER_INCOMING_REQUEST_POST_PROCESSED, order = 1)
  @SuppressWarnings("unused")
  public void addRequestIdToLoggingContext(
      @Nullable final ServletRequestDetails servletRequestDetails) {
    if (servletRequestDetails != null) {
      DiagnosticContext.fromRequest(servletRequestDetails, jsonParser).configureScope();
    } else {
      DiagnosticContext.empty().configureScope();
      log.warn(
          "Diagnostic Context Interceptor invoked with missing request details. Diagnostic information not set.");
    }
  }
}

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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class adds diagnostic information about the current request to logging and Sentry contexts.
 *
 * @author John Grimes
 * @author Piotr Szul
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
          "Diagnostic context interceptor invoked with missing request details. Diagnostic "
              + "information not set.");
    }
  }
}

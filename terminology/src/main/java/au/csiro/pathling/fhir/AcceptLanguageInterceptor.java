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

package au.csiro.pathling.fhir;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.http.HttpHeaders;

/**
 * A HAPI FHIR interceptor that adds an Accept-Language header to outgoing HTTP requests.
 * <p>
 * This interceptor automatically sets the Accept-Language header on all outgoing FHIR client
 * requests, allowing clients to specify their preferred language for localised content such as
 * display names in terminology resources.
 * </p>
 * <p>
 * The interceptor is registered with HAPI FHIR's interceptor framework and will be invoked
 * on the {@link Pointcut#CLIENT_REQUEST} pointcut for all outgoing requests.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * FhirContext ctx = FhirContext.forR4();
 * IGenericClient client = ctx.newRestfulGenericClient(serverUrl);
 * client.registerInterceptor(new AcceptLanguageInterceptor("en-AU"));
 * </pre>
 *
 * @author Mark Burgess
 * @author John Grimes
 * @see ca.uhn.fhir.interceptor.api.Interceptor
 * @see ca.uhn.fhir.interceptor.api.Pointcut#CLIENT_REQUEST
 * @see org.apache.http.HttpHeaders#ACCEPT_LANGUAGE
 */
@Interceptor
public class AcceptLanguageInterceptor {

  @Nonnull
  private final String acceptLanguage;

  /**
   * Creates a new Accept-Language interceptor with the specified language preference.
   * <p>
   * The Accept-Language header value should follow RFC 7231 format, such as "en-US",
   * "en-AU", "fr", or "en-US,en;q=0.9,fr;q=0.8" for multiple languages with quality values.
   * </p>
   *
   * @param acceptLanguage the Accept-Language header value to add to requests
   */
  public AcceptLanguageInterceptor(@Nonnull final String acceptLanguage) {
    this.acceptLanguage = acceptLanguage;
  }

  /**
   * Handles outgoing client requests by adding the Accept-Language header.
   * <p>
   * This method is automatically invoked by the HAPI FHIR interceptor framework
   * when a client request is being prepared. It adds the configured Accept-Language
   * header to the request if the request is not null.
   * </p>
   *
   * @param httpRequest the outgoing HTTP request, may be null
   */
  @SuppressWarnings("unused")
  @Hook(Pointcut.CLIENT_REQUEST)
  public void handleClientRequest(@Nullable final IHttpRequest httpRequest) {
    if (httpRequest != null) {
      httpRequest.addHeader(HttpHeaders.ACCEPT_LANGUAGE, this.acceptLanguage);
    }
  }
}

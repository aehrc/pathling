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

package au.csiro.pathling.auth;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;

/**
 * {@link HttpRequestInterceptor}  that perform preemptive bearer token authentication.
 * <p>
 * The {@link CredentialsProvider} configured with the {@link HttpClientContext} should provide
 * {@link TokenCredentials} for scopes that require authentication.
 */
@Slf4j
public class TokenAuthRequestInterceptor implements HttpRequestInterceptor {

  /**
   * Creates a new instance of {@link TokenAuthRequestInterceptor}.
   */
  public TokenAuthRequestInterceptor() {
  }

  @Override
  public void process(@Nonnull final HttpRequest request, @Nonnull final HttpContext context)
      throws HttpException, IOException {
    final CredentialsProvider credentialsProvider = (CredentialsProvider) context.getAttribute(
        HttpClientContext.CREDS_PROVIDER);
    final HttpHost targetHost = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
    if (credentialsProvider != null && targetHost != null) {
      final Optional<Credentials> maybeCredentials = Optional.ofNullable(
          credentialsProvider.getCredentials(new AuthScope(targetHost)));

      maybeCredentials.filter(TokenCredentials.class::isInstance)
          .map(TokenCredentials.class::cast)
          .map(TokenCredentials::getToken)
          .ifPresent(authToken -> {
            log.debug("Adding access token: {} to request: {}", authToken,
                request.getRequestLine());
            request.addHeader(HttpHeaders.AUTHORIZATION,
                AuthConst.AUTH_BEARER + " " + authToken.getAccessToken());
          });
    }
  }
}

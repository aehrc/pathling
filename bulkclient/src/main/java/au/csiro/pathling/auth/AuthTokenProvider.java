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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;

@Slf4j
public class AuthTokenProvider {

  @Nonnull
  private final HttpClient httpClient;

  private final long tokenExpiryTolerance;

  @Nonnull
  private final Map<ClientAuthMethod.AccessScope, AccessContext> accessContexts = new HashMap<>();

  public AuthTokenProvider(@Nonnull final HttpClient httpClient, final long tokenExpiryTolerance) {
    this.httpClient = httpClient;
    this.tokenExpiryTolerance = tokenExpiryTolerance;
  }

  /**
   * Gets access token for the given credentials.
   *
   * @param authMethod the authentication methods to use
   * @return the current token
   */
  public
  @Nonnull
  Token getToken(@Nonnull final ClientAuthMethod authMethod) {
    try {
      final AccessContext accessContext = ensureAccessContext(authMethod, tokenExpiryTolerance);
      // Now we should have a valid token, so we can add it to the request.
      return Token.of(
          requireNonNull(accessContext.getClientCredentialsResponse().getAccessToken()));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private AccessContext ensureAccessContext(@Nonnull final ClientAuthMethod credentials,
      final long tokenExpiryTolerance)
      throws IOException {
    synchronized (accessContexts) {
      final ClientAuthMethod.AccessScope accessScope = credentials.getAccessScope();
      AccessContext accessContext = accessContexts.get(accessScope);
      if (accessContext == null || accessContext.getExpiryTime()
          .isBefore(Instant.now().plusSeconds(tokenExpiryTolerance))) {
        // We need to get a new token if:
        // (1) We don't have a token yet;
        // (2) The token is expired, or;
        // (3) The token is about to expire (within the tolerance).
        log.debug("Getting new token for: {}", accessScope);
        accessContext = getNewAccessContext(credentials,
            tokenExpiryTolerance);
        accessContexts.put(accessScope, accessContext);
      }
      return accessContext;
    }
  }

  @Nonnull
  private AccessContext getNewAccessContext(@Nonnull final ClientAuthMethod authParams,
      final long tokenExpiryTolerance) throws IOException {
    final ClientCredentialsResponse response = clientCredentialsGrant(authParams,
        tokenExpiryTolerance);
    final Instant expires = getExpiryTime(response);
    log.debug("New token will expire at {}", expires);
    return new AccessContext(response, expires);
  }

  @Nonnull
  private ClientCredentialsResponse clientCredentialsGrant(
      @Nonnull final ClientAuthMethod authParams, final long tokenExpiryTolerance)
      throws IOException {
    final ClientCredentialsResponse grant = authParams.requestClientCredentials(httpClient);
    if (grant.getExpiresIn() < tokenExpiryTolerance) {
      throw new ClientProtocolException(
          "Client credentials grant expiry is less than the tolerance: " + grant.getExpiresIn());
    }
    return grant;
  }

  private static Instant getExpiryTime(@Nonnull final ClientCredentialsResponse response) {
    return Instant.now().plusSeconds(response.getExpiresIn());
  }

  public void clearAccessContexts() {
    synchronized (accessContexts) {
      accessContexts.clear();
    }
  }

  @Value
  static class AccessContext {

    @Nonnull
    ClientCredentialsResponse clientCredentialsResponse;

    @Nonnull
    Instant expiryTime;
  }
}
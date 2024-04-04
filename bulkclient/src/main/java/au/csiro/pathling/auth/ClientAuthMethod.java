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

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.AuthConfiguration;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.http.client.HttpClient;

/**
 * Authentication method for one of the FHIR SMART client authentication profiles.
 */

public interface ClientAuthMethod {

  /**
   * Represents the access scope for this method. Client Credentials for methofs with the same
   * access scope can be reused.
   */
  @Value
  class AccessScope {

    @Nonnull
    String tokenEndpoint;

    @Nonnull
    String clientId;

    @Nullable
    String scope;
  }

  /**
   * Gets the access scope for these credentials.
   *
   * @return the access scope
   */
  @Nonnull
  default AccessScope getAccessScope() {
    return new AccessScope(getTokenEndpoint(), getClientId(), getScope());
  }

  /**
   * Gets the client ID.
   *
   * @return the client ID
   */
  @Nonnull
  String getClientId();

  /**
   * Gets the token endpoint URL.
   */
  @Nonnull
  String getTokenEndpoint();

  /**
   * Gets the scope.
   */
  @Nullable
  String getScope();

  /**
   * Request Client Credentials Grant with this method using the provided HTTP client.
   *
   * @param httpClient the HTTP client to use
   * @return the response from the token endpoint
   * @throws IOException if an error occurs
   */
  @Nonnull
  ClientCredentialsResponse requestClientCredentials(@Nonnull final HttpClient httpClient)
      throws IOException;

  /**
   * Creates a new client authentication method from the given configuration. It assumes
   * 'client-confidential-asymmetric' if `privateKey` is set in the configuration and
   * 'client-confidential-symmetric' otherwise.
   * <p>
   * It's assumed (but not checked that the authentication is enabled).
   *
   * @param tokenEndpoint the token endpoint URL
   * @param authConfig the authentication configuration
   * @return the new client authentication method
   */
  @Nonnull
  static ClientAuthMethod create(@Nonnull final String tokenEndpoint,
      @Nonnull final AuthConfiguration authConfig) {
    if (nonNull(authConfig.getPrivateKeyJWK())) {
      return AsymmetricClientAuthMethod.builder()
          .tokenEndpoint(tokenEndpoint)
          .clientId(requireNonNull(authConfig.getClientId()))
          .privateKeyJWK(requireNonNull(authConfig.getPrivateKeyJWK()))
          .scope(authConfig.getScope())
          .build();
    } else {
      return SymmetricClientAuthMethod.builder()
          .tokenEndpoint(tokenEndpoint)
          .clientId(requireNonNull(authConfig.getClientId()))
          .clientSecret(requireNonNull(authConfig.getClientSecret()))
          .scope(authConfig.getScope())
          .sendClientCredentialsInBody(authConfig.isUseFormForBasicAuth())
          .build();
    }
  }
}

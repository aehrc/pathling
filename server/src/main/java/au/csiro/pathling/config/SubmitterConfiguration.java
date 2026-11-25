/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.config;

import au.csiro.fhir.auth.AuthConfig;
import au.csiro.pathling.operations.bulksubmit.SubmitterIdentifier;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Configuration for an allowed bulk submit submitter, including optional OAuth credentials for
 * authenticated file downloads.
 *
 * @param system the identifier system
 * @param value the identifier value
 * @param clientId the OAuth client ID
 * @param clientSecret the OAuth client secret
 * @param privateKeyJwk the private key JWK for asymmetric auth
 * @param scope the OAuth scope to request
 * @param tokenExpiryTolerance the token expiry tolerance in seconds
 * @param useFormForBasicAuth whether to use form encoding for basic auth
 * @param allowableSources URL prefixes that are allowed as sources for this submitter, replacing
 *     the shared {@code pathling.bulkSubmit.allowableSources} list when not empty
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record SubmitterConfiguration(
    @Nonnull String system,
    @Nonnull String value,
    @Nullable String clientId,
    @Nullable String clientSecret,
    @Nullable String privateKeyJwk,
    @Nullable String scope,
    @Nullable Long tokenExpiryTolerance,
    @Nullable Boolean useFormForBasicAuth,
    @Nonnull List<String> allowableSources) {

  /** Default token expiry tolerance in seconds. */
  public static final long DEFAULT_TOKEN_EXPIRY_TOLERANCE = 120;

  /**
   * Creates a new submitter configuration, treating an absent list of allowable sources as empty.
   * Spring binds a list that is missing from the configuration as null.
   */
  public SubmitterConfiguration {
    allowableSources = allowableSources == null ? List.of() : List.copyOf(allowableSources);
  }

  /**
   * Creates a SubmitterIdentifier from this configuration.
   *
   * @return the submitter identifier.
   */
  @Nonnull
  public SubmitterIdentifier toIdentifier() {
    return new SubmitterIdentifier(system, value);
  }

  /**
   * Checks if this submitter has OAuth credentials configured.
   *
   * @return true if clientId and either clientSecret or privateKeyJwk are configured.
   */
  public boolean hasCredentials() {
    return clientId != null && (clientSecret != null || privateKeyJwk != null);
  }

  /**
   * Creates an AuthConfig from this configuration for use with the fhir-auth library.
   *
   * @param tokenEndpoint the discovered token endpoint URL.
   * @return an AuthConfig instance configured with this submitter's credentials.
   */
  @Nonnull
  public AuthConfig toAuthConfig(@Nonnull final String tokenEndpoint) {
    return AuthConfig.builder()
        .enabled(true)
        .useSMART(false)
        .tokenEndpoint(tokenEndpoint)
        .clientId(clientId)
        .clientSecret(clientSecret)
        .privateKeyJWK(privateKeyJwk)
        .scope(scope)
        .tokenExpiryTolerance(
            tokenExpiryTolerance != null ? tokenExpiryTolerance : DEFAULT_TOKEN_EXPIRY_TOLERANCE)
        // SMART Backend Services spec expects credentials in form body for symmetric auth.
        .useFormForBasicAuth(useFormForBasicAuth == null || useFormForBasicAuth)
        .build();
  }

  /**
   * Returns a string form of this configuration that omits {@code clientSecret} and {@code
   * privateKeyJwk}, as it is written to the log when the server configuration is logged.
   *
   * @return the string form of this configuration, without credentials
   */
  @Override
  @Nonnull
  public String toString() {
    return "SubmitterConfiguration[system="
        + system
        + ", value="
        + value
        + ", clientId="
        + clientId
        + ", scope="
        + scope
        + ", tokenExpiryTolerance="
        + tokenExpiryTolerance
        + ", useFormForBasicAuth="
        + useFormForBasicAuth
        + "]";
  }
}

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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.http.Header;
import org.apache.http.message.BasicNameValuePair;

/**
 * Authentication method for one of the FHIR SMART client authentication profiles.
 */
public interface ClientAuthMethod {

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
   * Gets the authentication headers required for these credentials.
   *
   * @return the authentication headers
   */
  @Nonnull
  default List<Header> getAuthHeaders() {
    return Collections.emptyList();
  }

  /**
   * Gets the authentication parameters to be sent in the POST body form for these credentials.
   *
   * @return the authentication parameters
   */
  @Nonnull
  default List<BasicNameValuePair> getAuthParams() {
    return getAuthParams(Instant.now());
  }

  /**
   * Gets the authentication parameters to be sent in the POST body form for these credentials.
   *
   * @param now the current time (to be used to calculate expiry if necessary)
   * @return the authentication parameters
   */
  @Nonnull
  List<BasicNameValuePair> getAuthParams(@Nonnull final Instant now);

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
   * Represents the access scope for these credentials. Credentials with the same access scope can
   * be reused.
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
}

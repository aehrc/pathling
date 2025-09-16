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

package org.springframework.security.oauth2.jwt;

import jakarta.annotation.Nonnull;

import java.util.Map;

/**
 * Provides access to functionality within the package-private
 * {@link JwtDecoderProviderConfigurationUtils} class.
 *
 * @author John Grimes
 */
public class JwtDecoderProviderConfigurationUtilsProxy {

  /**
   * @param issuer the issuer required to be asserted within the token
   * @return a {@link Map} containing the OIDC configuration values
   */
  public static Map<String, Object> getConfigurationForIssuerLocation(
      @Nonnull final String issuer) {
    return JwtDecoderProviderConfigurationUtils.getConfigurationForIssuerLocation(issuer);
  }

}

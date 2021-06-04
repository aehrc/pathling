/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package org.springframework.security.oauth2.jwt;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Provides access to functionality within the package-private {@link
 * JwtDecoderProviderConfigurationUtils} class.
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

  /**
   * @param configuration the OIDC configuration values
   * @param issuer the issuer required to be asserted within the token
   */
  public static void validateIssuer(@Nonnull final Map<String, Object> configuration,
      @Nonnull final String issuer) {
    JwtDecoderProviderConfigurationUtils.validateIssuer(configuration, issuer);
  }

}

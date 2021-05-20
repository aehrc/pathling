/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package org.springframework.security.oauth2.jwt;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.Configuration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.stereotype.Component;

/**
 * Retrieves and provides access to discoverable configuration from the OIDC endpoint.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Getter
public class OidcConfiguration {

  private final NimbusJwtDecoder jwtDecoder;
  private final Map<String, Object> oidcConfiguration;

  /**
   * @param configuration A {@link Configuration} instance which controls the behaviour of the
   * server
   */
  public OidcConfiguration(@Nonnull final Configuration configuration) {
    final Supplier<RuntimeException> authConfigError = () -> new RuntimeException(
        "Configuration for issuer must be present if authorization is enabled");
    final String issuer = configuration.getAuth().getIssuer().orElseThrow(authConfigError);

    oidcConfiguration = JwtDecoderProviderConfigurationUtils
        .getConfigurationForIssuerLocation(issuer);
    jwtDecoder = withProviderConfiguration(oidcConfiguration, issuer);
  }

  @Nonnull
  private static NimbusJwtDecoder withProviderConfiguration(
      @Nonnull final Map<String, Object> configuration, @Nonnull final String issuer) {
    JwtDecoderProviderConfigurationUtils.validateIssuer(configuration, issuer);
    final OAuth2TokenValidator<Jwt> jwtValidator = JwtValidators.createDefaultWithIssuer(issuer);
    final NimbusJwtDecoder jwtDecoder = NimbusJwtDecoder
        .withJwkSetUri(configuration.get("jwks_uri").toString()).build();
    jwtDecoder.setJwtValidator(jwtValidator);
    return jwtDecoder;
  }

  /**
   * @param item the {@link ConfigItem} to retrieve
   * @return the value, if present
   */
  @Nonnull
  public Optional<String> get(@Nonnull final ConfigItem item) {
    final Object value = oidcConfiguration.get(item.getKey());
    if (value != null) {
      check(value instanceof String);
    }
    return Optional.ofNullable((String) value);
  }

  /**
   * OIDC configuration items.
   */
  @Getter
  public enum ConfigItem {
    /**
     * Key used for the authorization URL.
     */
    AUTH_URL("authorization_endpoint"),

    /**
     * Key used for the token URL.
     */
    TOKEN_URL("token_endpoint"),

    /**
     * Key used for the token revocation URL.
     */
    REVOKE_URL("revocation_endpoint");

    @Nonnull
    private final String key;

    ConfigItem(@Nonnull final String key) {
      this.key = key;
    }

  }

}

/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.springframework.security.oauth2.jwt.JwtDecoderProviderConfigurationUtilsProxy.getConfigurationForIssuerLocation;
import static org.springframework.security.oauth2.jwt.JwtDecoderProviderConfigurationUtilsProxy.validateIssuer;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Authorization;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
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

  private final JwtDecoder jwtDecoder;
  private final JwtAuthenticationConverter jwtAuthenticationConverter;
  private final Map<String, Object> oidcConfiguration;

  /**
   * @param configuration A {@link Configuration} instance which controls the behaviour of the
   * server
   */
  public OidcConfiguration(@Nonnull final Configuration configuration) {
    final Supplier<RuntimeException> authConfigError = () -> new RuntimeException(
        "Configuration for issuer must be present if authorization is enabled");
    final String issuer = configuration.getAuth().getIssuer().orElseThrow(authConfigError);

    oidcConfiguration = getConfigurationForIssuerLocation(issuer);
    jwtAuthenticationConverter = buildJwtAuthenticationConverter();
    jwtDecoder = buildJwtDecoder(configuration, oidcConfiguration);
  }

  @Nonnull
  private static JwtDecoder buildJwtDecoder(@Nonnull final Configuration configuration,
      @Nonnull final Map<String, Object> oidcConfiguration) {
    final Authorization authConfig = configuration.getAuth();
    final Supplier<RuntimeException> authConfigError = () -> new RuntimeException(
        "Configuration for issuer and audience must be present if authorization is enabled");
    final String issuer = authConfig.getIssuer().orElseThrow(authConfigError);
    final String audience = authConfig.getAudience().orElseThrow(authConfigError);

    // Audience and issuer within each incoming bearer token are validated against the values
    // configured into the server.
    final OAuth2TokenValidator<Jwt> withAudience = new JwtAudienceValidator(audience);
    final OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
    final OAuth2TokenValidator<Jwt> validator = new DelegatingOAuth2TokenValidator<>(withIssuer,
        withAudience);

    // Information for decoding, such as the signing key, is auto discovered from the OIDC
    // configuration endpoint. We don't currently support non-OIDC authorization servers.
    final NimbusJwtDecoder jwtDecoder = withProviderConfiguration(oidcConfiguration, issuer);
    jwtDecoder.setJwtValidator(validator);
    return jwtDecoder;
  }

  @Nonnull
  private static JwtAuthenticationConverter buildJwtAuthenticationConverter() {
    final JwtGrantedAuthoritiesConverter converter = new JwtGrantedAuthoritiesConverter();
    converter.setAuthoritiesClaimName("authorities");
    converter.setAuthorityPrefix("");

    final JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
    jwtConverter.setJwtGrantedAuthoritiesConverter(converter);
    return jwtConverter;
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
   * @param oidcConfiguration an OidcConfiguration object containing configuration retrieved from
   * OIDC discovery
   * @return a {@link JwtDecoder}
   */
  @Bean
  @Nonnull
  public static JwtDecoder jwtDecoder(@Nonnull final OidcConfiguration oidcConfiguration) {
    return oidcConfiguration.getJwtDecoder();
  }

  /**
   * @param oidcConfiguration an OidcConfiguration object containing configuration retrieved from
   * OIDC discovery
   * @return a {@link JwtAuthenticationConverter}
   */
  @Bean
  @Nonnull
  public static JwtAuthenticationConverter jwtAuthenticationConverter(
      @Nonnull final OidcConfiguration oidcConfiguration) {
    return oidcConfiguration.getJwtAuthenticationConverter();
  }

  @Nonnull
  private static NimbusJwtDecoder withProviderConfiguration(
      @Nonnull final Map<String, Object> configuration, @Nonnull final String issuer) {
    validateIssuer(configuration, issuer);
    final OAuth2TokenValidator<Jwt> jwtValidator = JwtValidators.createDefaultWithIssuer(issuer);
    final NimbusJwtDecoder jwtDecoder = NimbusJwtDecoder
        .withJwkSetUri(configuration.get("jwks_uri").toString()).build();
    jwtDecoder.setJwtValidator(jwtValidator);
    return jwtDecoder;
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

/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.springframework.security.oauth2.jwt.JwtDecoderProviderConfigurationUtilsProxy.getConfigurationForIssuerLocation;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.config.AuthorizationConfiguration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Retrieves and provides access to discoverable configuration from the OIDC endpoint.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Getter
public class OidcConfiguration {

  private final Map<String, Object> oidcConfiguration;

  /**
   * @param configuration A {@link Configuration} instance which controls the behaviour of the
   * server
   */
  @Autowired
  public OidcConfiguration(@Nonnull final Configuration configuration) {
    final AuthorizationConfiguration authConfig = configuration.getAuth();
    final Supplier<RuntimeException> authConfigError = () -> new RuntimeException(
        "Configuration for issuer must be present if authorization is enabled");
    final String issuer = authConfig.getIssuer().orElseThrow(authConfigError);

    oidcConfiguration = getConfigurationForIssuerLocation(issuer);
  }

  /**
   * This constructor is used in cases where we have not configured the issuer ahead of time, or if
   * there are multiple potential issuers.
   *
   * @param issuer a specified value for the issuer
   */
  public OidcConfiguration(@Nonnull final String issuer) {
    oidcConfiguration = getConfigurationForIssuerLocation(issuer);
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
    REVOKE_URL("revocation_endpoint"),

    /**
     * A JSON Web Key Set (JWKS) URL that contains the public keys used to verify the signature.
     */
    JWKS_URI("jwks_uri");

    @Nonnull
    private final String key;

    ConfigItem(@Nonnull final String key) {
      this.key = key;
    }

  }

}

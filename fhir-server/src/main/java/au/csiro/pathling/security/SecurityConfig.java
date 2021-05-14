package au.csiro.pathling.security;

import au.csiro.pathling.Configuration;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;

/**
 * Web security configuration for Pathling.
 * <p>
 * See: https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization
 * <p>
 * See (CSRF) : https://stackoverflow.com/questions/51079564/spring-security-antmatchers-not-being-applied-on-post-requests-and-only-works-wi/51088555
 */
@EnableWebSecurity
@Profile("server")
@Slf4j
public class SecurityConfig extends WebSecurityConfigurerAdapter {

  @Value("${pathling.auth.enabled}")
  private boolean authEnabled;

  @Autowired
  private Configuration configuration;

  @Override
  protected void configure(HttpSecurity http) throws Exception {

    if (authEnabled) {
      final Configuration.Authorisation authConfig = configuration.getAuth();
      log.info("Request authentication is enabled, with config: {}", authConfig);
      http.authorizeRequests()
          // GET requests that do not require authentication
          .mvcMatchers(HttpMethod.GET,
              "/fhir/metadata",
              "/fhir/OperationDefinition/**",
              "/fhir/.well-known/**")
          .permitAll()
          // Anything else needs to be authenticated
          .anyRequest()
          .authenticated()
          .and()
          .oauth2ResourceServer()
          .jwt()
          .decoder(jwtDecoder(authConfig))
          .jwtAuthenticationConverter(jwtAuthenticationConverter());
    } else {
      log.warn("Request authentication is disabled.");
      // permit all requests including these
      // without authentication
      http
          // TODO: do we want this protection enabled?
          // (without this POST requests fail with 403 Forbidden)
          .csrf().disable()
          .authorizeRequests().anyRequest().permitAll();
    }
  }

  JwtDecoder jwtDecoder(@Nonnull final Configuration.Authorisation authConfig) {

    final Supplier<RuntimeException> authConfigError = () -> new RuntimeException(
        "Configuration for issuer and audience must be present if authorisation is enabled");
    final String issuer = authConfig.getIssuer().orElseThrow(authConfigError);
    final String audience = authConfig.getAudience().orElseThrow(authConfigError);

    OAuth2TokenValidator<Jwt> withAudience = new JwtAudienceValidator(audience);
    OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
    OAuth2TokenValidator<Jwt> validator = new DelegatingOAuth2TokenValidator<>(withIssuer,
        withAudience);

    // TODO: OIDC info includes all the URLs (revoke, auth, etc)
    final NimbusJwtDecoder jwtDecoder;
    if (authConfig.isEnableOcid()) {
      jwtDecoder = (NimbusJwtDecoder) JwtDecoders.fromOidcIssuerLocation(issuer);
    } else {
      // TODO: Fix error message
      final String jwksUrl = authConfig.getJwksUrl().orElseThrow(authConfigError);
      // TODO: This needs to be reviewd
      jwtDecoder = NimbusJwtDecoder.withJwkSetUri(jwksUrl).build();
    }
    jwtDecoder.setJwtValidator(validator);
    return jwtDecoder;
  }

  JwtAuthenticationConverter jwtAuthenticationConverter() {
    JwtGrantedAuthoritiesConverter converter = new JwtGrantedAuthoritiesConverter();
    converter.setAuthoritiesClaimName("scope");
    converter.setAuthorityPrefix("");

    JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
    jwtConverter.setJwtGrantedAuthoritiesConverter(converter);
    return jwtConverter;
  }
}
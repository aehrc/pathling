package au.csiro.pathling.security;

import au.csiro.pathling.Configuration;
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
 * See: https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization
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
      log.info("Request authentication is enabled, with config: {}", configuration.getAuth());
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
          .decoder(jwtDecoder())
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

  // CorsConfigurationSource corsConfigurationSource() {
  //   CorsConfiguration configuration = new CorsConfiguration();
  //   configuration.setAllowedMethods(Arrays.asList(
  //       HttpMethod.GET.name(),
  //       HttpMethod.PUT.name(),
  //       HttpMethod.POST.name(),
  //       HttpMethod.DELETE.name()
  //   ));
  //
  //   UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
  //   source.registerCorsConfiguration("/**", configuration.applyPermitDefaultValues());
  //   return source;
  // }

  JwtDecoder jwtDecoder() {
    final String issuer = "https://dev-ztwhmd51.au.auth0.com/";
    //OAuth2TokenValidator<Jwt> withAudience = new AudienceValidator(audience);
    OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
    OAuth2TokenValidator<Jwt> validator = new DelegatingOAuth2TokenValidator<>(withIssuer);

    NimbusJwtDecoder jwtDecoder = (NimbusJwtDecoder) JwtDecoders.fromOidcIssuerLocation(issuer);
    jwtDecoder.setJwtValidator(validator);
    return jwtDecoder;
  }

  JwtAuthenticationConverter jwtAuthenticationConverter() {
    JwtGrantedAuthoritiesConverter converter = new JwtGrantedAuthoritiesConverter();
    converter.setAuthoritiesClaimName("permissions");
    converter.setAuthorityPrefix("");

    JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
    jwtConverter.setJwtGrantedAuthoritiesConverter(converter);
    return jwtConverter;
  }
}
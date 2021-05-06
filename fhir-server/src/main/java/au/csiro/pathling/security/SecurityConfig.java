package au.csiro.pathling.security;

import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;

@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class SecurityConfig extends WebSecurityConfigurerAdapter {
  // @Value("${auth0.audience}")
  // private String audience;
  //
  // @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
  // private String issuer;

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.authorizeRequests()
        .mvcMatchers(HttpMethod.GET, "/**").permitAll() // GET requests don't need auth
        .anyRequest()
        .authenticated()
        .and()
        .oauth2ResourceServer()
        .jwt()
        .decoder(jwtDecoder())
        .jwtAuthenticationConverter(jwtAuthenticationConverter());
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
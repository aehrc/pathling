/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Authorization;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * Web security configuration for Pathling.
 *
 * @see <a href="https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization">Spring
 * Security 5 Java API: Authorization</a>
 * @see <a href="https://stackoverflow.com/questions/51079564/spring-security-antmatchers-not-being-applied-on-post-requests-and-only-works-wi/51088555">Spring
 * security antMatchers not being applied on POST requests and only works with GET</a>
 */
@EnableWebSecurity
@Profile("server")
@Slf4j
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {

  @Value("${pathling.auth.enabled}")
  private boolean authEnabled;

  @Autowired
  private Configuration configuration;

  @Autowired
  private Optional<OidcConfiguration> oidcConfiguration;

  @Autowired
  private Optional<JwtDecoder> jwtDecoder;

  @Override
  protected void configure(final HttpSecurity http) throws Exception {
    if (authEnabled) {
      final Authorization authConfig = configuration.getAuth();
      final OidcConfiguration checkedOidcConfig = checkPresent(oidcConfiguration);
      log.info("Request authentication is enabled, with config: {}", authConfig);

      http.authorizeRequests()
          // The following requests do not require authentication.
          .mvcMatchers(HttpMethod.GET,
              "/fhir/metadata",   // Server capabilities operation
              "/fhir/OperationDefinition/**",  // GET on OperationDefinition resources
              "/fhir/.well-known/**")          // SMART configuration endpoint
          .permitAll()
          // Anything else needs to be authenticated.
          .anyRequest()
          .authenticated()
          .and()
          .oauth2ResourceServer()
          .jwt()
          .decoder(checkPresent(jwtDecoder))
          .jwtAuthenticationConverter(checkedOidcConfig.getJwtAuthenticationConverter());

    } else {
      log.info("Request authentication is disabled");
      http
          // Without this POST requests fail with 403 Forbidden.
          .csrf().disable()
          .authorizeRequests().anyRequest().permitAll();
    }
  }

}

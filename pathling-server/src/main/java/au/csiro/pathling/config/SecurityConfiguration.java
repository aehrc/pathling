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

package au.csiro.pathling.config;

import static au.csiro.pathling.utilities.Preconditions.check;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

/** 
 * Web security configuration for Pathling.
 *
 * @see <a
 * href="https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization">Spring
 * Security 5 Java API: Authorization</a>
 * @see <a
 * href="https://stackoverflow.com/questions/51079564/spring-security-antmatchers-not-being-applied-on-post-requests-and-only-works-wi/51088555">Spring
 * security antMatchers not being applied on POST requests and only works with GET</a>
 */
@Configuration
@EnableWebSecurity
@Profile("server")
@Slf4j
public class SecurityConfiguration {

  @Nonnull
  private final ServerConfiguration configuration;

  @Nullable
  private final JwtAuthenticationConverter authenticationConverter;

  @Nullable
  private final JwtDecoder jwtDecoder;

  @Value("${pathling.auth.enabled}")
  private boolean authEnabled;

  /**
   * Constructs a new {@link SecurityConfiguration} object.
   *
   * @param configuration a {@link ServerConfiguration} object
   */
  public SecurityConfiguration(@Nonnull final ServerConfiguration configuration,
      @Nullable final JwtAuthenticationConverter authenticationConverter,
      @Nullable final JwtDecoder jwtDecoder) {
    this.configuration = configuration;
    this.authenticationConverter = authenticationConverter;
    this.jwtDecoder = jwtDecoder;
  }

  /**
   * Configures the security filter chain.
   *
   * @param http the {@link HttpSecurity} object
   * @return the security filter chain
   * @throws Exception if an error occurs
   */
  @Bean
  public SecurityFilterChain securityFilterChain(@Nonnull final HttpSecurity http)
      throws Exception {
    if (authEnabled) {
      check(authenticationConverter != null,
          "Authentication converter must be provided when authentication is enabled");
      check(jwtDecoder != null, "JWT decoder must be provided when authentication is enabled");
      http.authorizeHttpRequests(authz -> authz
              // The following requests do not require authentication.
              .requestMatchers("/actuator/**").permitAll()
              .requestMatchers(HttpMethod.GET, "/fhir/metadata").permitAll()
              .requestMatchers(HttpMethod.GET, "/fhir/OperationDefinition/**").permitAll()
              .requestMatchers(HttpMethod.GET, "/fhir/.well-known/**").permitAll()
              // Anything else needs to be authenticated.
              .anyRequest().authenticated())
          // Enable CORS as per the configuration.
          .cors(cors -> cors.configurationSource(corsConfigurationSource()))
          // Use the provided JWT decoder and authentication converter.
          .oauth2ResourceServer(oauth2 -> oauth2
              .jwt(jwt -> jwt
                  .jwtAuthenticationConverter(authenticationConverter)
                  .decoder(jwtDecoder)
              ));

    } else {
      http.authorizeHttpRequests(authz -> authz.anyRequest().permitAll())
          // Enable CORS as per the configuration.
          .cors(cors -> cors.configurationSource(corsConfigurationSource()))
          // Without this POST requests fail with 403 Forbidden.
          .csrf(AbstractHttpConfigurer::disable);
    }

    return http.build();
  }

  /**
   * Constructs Spring CORS configuration.
   *
   * @return CORS configuration source
   */
  public CorsConfigurationSource corsConfigurationSource() {
    final CorsConfiguration cors = new CorsConfiguration();
    
    cors.setAllowedOrigins(configuration.getCors().getAllowedOrigins());
    cors.setAllowedOriginPatterns(configuration.getCors().getAllowedOriginPatterns());
    cors.setAllowedMethods(configuration.getCors().getAllowedMethods());
    cors.setAllowedHeaders(configuration.getCors().getAllowedHeaders());
    cors.setExposedHeaders(configuration.getCors().getExposedHeaders());
    cors.setMaxAge(configuration.getCors().getMaxAge());
    cors.setAllowCredentials(configuration.getAuth().isEnabled());

    final UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", cors);
    return source;
  }

}

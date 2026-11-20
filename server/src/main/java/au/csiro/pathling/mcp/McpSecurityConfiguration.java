/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
package au.csiro.pathling.mcp;

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.JWKS_URI;

import au.csiro.pathling.config.McpConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.PathlingJwtDecoderBuilder;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Secures the MCP endpoint as an OAuth 2.0 protected resource, as the MCP authorisation
 * specification and MCP clients such as Claude require.
 *
 * <p>An unauthenticated request is answered with a 401 whose {@code WWW-Authenticate} header points
 * at the protected resource metadata, from which the client discovers the authorisation server.
 * Access tokens must come from the MCP issuer, which may differ from the issuer that protects the
 * FHIR API, and must have been issued for the MCP endpoint itself. The authorities in the token are
 * applied to each tool exactly as they are to the equivalent FHIR operation.
 *
 * @author John Grimes
 * @see <a href="https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization">MCP
 *     authorization</a>
 */
@Slf4j
@Configuration
@ConditionalOnProperty(
    prefix = "pathling",
    name = {"auth.enabled", "mcp.enabled"},
    havingValue = "true")
public class McpSecurityConfiguration {

  /**
   * Creates the security filter chain for the MCP endpoint and its metadata, which takes precedence
   * over the chain that secures the rest of the server.
   *
   * @param http the security builder
   * @param configuration the server configuration
   * @param oidcConfiguration the discovery document of the server's main issuer
   * @param decoderBuilder builds JWT decoders with Pathling's key selection
   * @param authenticationConverter reads Pathling authorities from the token
   * @return the filter chain
   * @throws Exception if the chain cannot be built
   */
  @Bean
  @Order(Ordered.HIGHEST_PRECEDENCE)
  public SecurityFilterChain mcpSecurityFilterChain(
      @Nonnull final HttpSecurity http,
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final OidcConfiguration oidcConfiguration,
      @Nonnull final PathlingJwtDecoderBuilder decoderBuilder,
      @Nonnull final JwtAuthenticationConverter authenticationConverter)
      throws Exception {
    final McpConfiguration mcp = configuration.getMcp();
    if (mcp.getResourceUrl().isEmpty()) {
      // The derived URL comes from the Host and forwarding headers, which the caller controls, so
      // the audience check would accept a token issued for any other MCP endpoint served by the
      // same issuer.
      log.warn(
          "pathling.mcp.resourceUrl is not set, so the MCP endpoint derives its URL from request"
              + " headers and will accept tokens issued for other MCP endpoints by the same"
              + " issuer. Set it to the public URL of the endpoint.");
    }
    final JwtDecoder decoder = decoder(configuration, oidcConfiguration, decoderBuilder);
    final AuthenticationEntryPoint entryPoint = new ChallengeEntryPoint(mcp);

    http.securityMatcher(
            McpResourceUrl.ENDPOINT_PATH,
            McpResourceUrl.METADATA_PATH,
            McpResourceUrl.METADATA_PATH + "/**")
        .authorizeHttpRequests(
            authz ->
                authz
                    .requestMatchers(
                        McpResourceUrl.METADATA_PATH, McpResourceUrl.METADATA_PATH + "/**")
                    .permitAll()
                    .anyRequest()
                    .authenticated())
        // The server's CORS configuration is found by its bean name, corsConfigurationSource.
        .cors(Customizer.withDefaults())
        // Every request carries its own bearer token, so there is no session to protect.
        .csrf(AbstractHttpConfigurer::disable)
        .sessionManagement(
            session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint(entryPoint))
        .oauth2ResourceServer(
            oauth2 ->
                oauth2
                    .authenticationEntryPoint(entryPoint)
                    .jwt(
                        jwt ->
                            jwt.decoder(decoder)
                                .jwtAuthenticationConverter(authenticationConverter)));
    return http.build();
  }

  /**
   * Returns the issuer whose tokens are accepted at the MCP endpoint: the configured MCP issuer, or
   * otherwise the server's main issuer.
   *
   * @param configuration the server configuration
   * @return the issuer
   * @throws IllegalStateException if neither issuer is configured
   */
  @Nonnull
  public static String issuer(@Nonnull final ServerConfiguration configuration) {
    return configuration
        .getMcp()
        .getAuth()
        .getIssuer()
        .or(() -> configuration.getAuth().getIssuer())
        .orElseThrow(
            () -> new IllegalStateException("An issuer must be configured for the MCP endpoint"));
  }

  @Nonnull
  private static JwtDecoder decoder(
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final OidcConfiguration oidcConfiguration,
      @Nonnull final PathlingJwtDecoderBuilder decoderBuilder) {
    final String issuer = issuer(configuration);
    // A separate MCP issuer publishes its own keys, found through its own discovery document.
    final OidcConfiguration issuerConfiguration =
        configuration.getMcp().getAuth().getIssuer().isPresent()
            ? new OidcConfiguration(issuer)
            : oidcConfiguration;
    final String jwksUri =
        issuerConfiguration
            .get(JWKS_URI)
            .orElseThrow(
                () -> new IllegalStateException("The MCP issuer does not publish a jwks_uri"));
    final McpConfiguration mcp = configuration.getMcp();
    return decoderBuilder.build(
        jwksUri,
        List.of(
            new JwtTimestampValidator(),
            new JwtIssuerValidator(issuer),
            new McpAudienceValidator(() -> McpResourceUrl.resolve(mcp, currentRequest()))));
  }

  @Nonnull
  private static HttpServletRequest currentRequest() {
    return ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes())
        .getRequest();
  }

  /**
   * Issues the standard bearer token challenge, extended with the location of the protected
   * resource metadata and the scopes to request, so that a client can find the authorisation server
   * and sign in.
   */
  private static class ChallengeEntryPoint implements AuthenticationEntryPoint {

    @Nonnull private final McpConfiguration configuration;

    @Nonnull
    private final BearerTokenAuthenticationEntryPoint delegate =
        new BearerTokenAuthenticationEntryPoint();

    private ChallengeEntryPoint(@Nonnull final McpConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public void commence(
        final HttpServletRequest request,
        final HttpServletResponse response,
        final AuthenticationException authException)
        throws IOException {
      delegate.commence(request, response, authException);
      final String metadataUrl;
      try {
        metadataUrl = McpResourceUrl.metadataUrl(McpResourceUrl.resolve(configuration, request));
      } catch (final IllegalArgumentException | IllegalStateException e) {
        // Forwarding headers that do not form a URL leave the plain challenge in place.
        log.debug("Unable to derive the MCP resource URL from the request", e);
        return;
      }
      final StringBuilder parameters =
          new StringBuilder("resource_metadata=\"").append(metadataUrl).append('"');
      final List<String> scopes = configuration.getAuth().getScopesSupported();
      if (!scopes.isEmpty()) {
        parameters.append(", scope=\"").append(String.join(" ", scopes)).append('"');
      }
      final String challenge = response.getHeader(HttpHeaders.WWW_AUTHENTICATE);
      response.setHeader(
          HttpHeaders.WWW_AUTHENTICATE,
          "Bearer".equals(challenge) ? "Bearer " + parameters : challenge + ", " + parameters);
    }
  }
}

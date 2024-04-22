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

package au.csiro.pathling.security;

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.JWKS_URI;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.jose.util.ResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTClaimsSetAwareJWSKeySelector;
import com.nimbusds.jwt.proc.JWTProcessor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

/**
 * @author John Grimes
 */
@Component
@Profile("server & !ga4gh")
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Primary
public class PathlingJwtDecoderBuilder implements JWTClaimsSetAwareJWSKeySelector<SecurityContext> {

  @Nonnull
  private final OidcConfiguration oidcConfiguration;

  @Nonnull
  private final RestOperations restOperations = new RestTemplate();

  /**
   * @param oidcConfiguration configuration used to instantiate the builder
   */
  public PathlingJwtDecoderBuilder(@Nonnull final OidcConfiguration oidcConfiguration) {
    this.oidcConfiguration = oidcConfiguration;
  }

  /**
   * @param configuration controls the behaviour of the resulting JWT decoder
   * @return a JWT decoder
   */
  public JwtDecoder build(@Nonnull final ServerConfiguration configuration) {
    final AuthorizationConfiguration auth = getAuthConfiguration(configuration);

    // Audience and issuer within each incoming bearer token are validated against the values
    // configured into the server.
    final List<OAuth2TokenValidator<Jwt>> validators = new ArrayList<>();
    auth.getIssuer().ifPresent(i -> validators.add(new JwtIssuerValidator(i)));
    auth.getAudience().ifPresent(a -> validators.add(new JwtAudienceValidator(a)));
    return buildDecoderWithValidators(validators);
  }

  @Nonnull
  protected AuthorizationConfiguration getAuthConfiguration(
      @Nullable final ServerConfiguration configuration) {
    checkArgument(configuration != null, "configuration cannot be null");
    final AuthorizationConfiguration auth = configuration.getAuth();
    check(auth.isEnabled());
    return auth;
  }

  @Override
  public List<? extends Key> selectKeys(@Nullable final JWSHeader header,
      @Nullable final JWTClaimsSet claimsSet, @Nullable final SecurityContext context)
      throws KeySourceException {
    checkArgument(claimsSet != null, "claimsSet cannot be null");
    final String jwksUri = getJwksUri(claimsSet);

    try {
      final JWKSource<SecurityContext> jwkSource = new RemoteJWKSet<>(
          new URL(jwksUri), new JwksRetriever(restOperations));
      final JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(
          JWSAlgorithm.RS256, jwkSource);
      return keySelector.selectJWSKeys(header, context);
    } catch (final IOException e) {
      throw new KeySourceException("Failed to retrieve keys from " + jwksUri, e);
    }
  }

  @Nonnull
  protected NimbusJwtDecoder buildDecoderWithValidators(
      @Nonnull final List<OAuth2TokenValidator<Jwt>> validators) {
    final OAuth2TokenValidator[] validatorsArray = validators.toArray(new OAuth2TokenValidator[0]);
    @SuppressWarnings("unchecked")
    final OAuth2TokenValidator<Jwt> validator = new DelegatingOAuth2TokenValidator<>(
        validatorsArray);

    final NimbusJwtDecoder jwtDecoder = new NimbusJwtDecoder(processor());
    jwtDecoder.setJwtValidator(validator);
    return jwtDecoder;
  }

  @Nonnull
  protected String getJwksUri(@Nonnull final JWTClaimsSet claimsSet) {
    return checkPresent(oidcConfiguration.get(JWKS_URI));
  }

  @Nonnull
  private JWTProcessor<SecurityContext> processor() {
    final ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
    jwtProcessor.setJWTClaimsSetAwareJWSKeySelector(this);
    return jwtProcessor;
  }

  private static class JwksRetriever implements ResourceRetriever {

    private static final MediaType APPLICATION_JWK_SET_JSON = new MediaType("application",
        "jwk-set+json");

    private final RestOperations restOperations;

    private JwksRetriever(@Nonnull final RestOperations restOperations) {
      this.restOperations = restOperations;
    }

    @Override
    public Resource retrieveResource(@Nullable final URL url) throws IOException {
      checkArgument(url != null, "url must not be null");
      final HttpHeaders headers = new HttpHeaders();
      headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, APPLICATION_JWK_SET_JSON));
      final ResponseEntity<String> response = getResponse(url, headers);
      if (!HttpStatusCode.valueOf(200).equals(response.getStatusCode())) {
        throw new IOException(response.toString());
      }
      if (response.getBody() == null) {
        throw new UnclassifiedServerFailureException(502, "Request for JWKS returned empty body");
      }
      return new Resource(response.getBody(), "UTF-8");
    }

    @Nonnull
    private ResponseEntity<String> getResponse(@Nonnull final URL url,
        @Nonnull final HttpHeaders headers) throws IOException {
      try {
        final RequestEntity<Void> request = new RequestEntity<>(headers, HttpMethod.GET,
            url.toURI());
        return this.restOperations.exchange(request, String.class);
      } catch (final Exception ex) {
        throw new IOException(ex);
      }
    }

  }
}

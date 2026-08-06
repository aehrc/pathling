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

package au.csiro.pathling.security;

import static au.csiro.pathling.security.JwsTestFixtures.claims;
import static au.csiro.pathling.security.JwsTestFixtures.generateEcKey;
import static au.csiro.pathling.security.JwsTestFixtures.generateRsaKey;
import static au.csiro.pathling.security.JwsTestFixtures.header;
import static au.csiro.pathling.security.JwsTestFixtures.jwksBody;
import static au.csiro.pathling.security.JwsTestFixtures.mockJwksTransport;
import static au.csiro.pathling.security.JwsTestFixtures.signToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import jakarta.annotation.Nonnull;
import java.security.Key;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;

/**
 * Tests for {@link PathlingJwtDecoderBuilder} covering auth configuration validation, decoder
 * building, and key selection error handling.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class PathlingJwtDecoderBuilderTest {

  private PathlingJwtDecoderBuilder builder;

  @BeforeEach
  void setUp() {
    // Create an OidcConfiguration with a test JWKS URI.
    final OidcConfiguration oidcConfig =
        new OidcConfiguration(Map.of("jwks_uri", "http://localhost/.well-known/jwks.json"));
    builder = new PathlingJwtDecoderBuilder(oidcConfig, configuration(List.of()));
  }

  // -- getAuthConfiguration --

  @Test
  void getAuthConfigurationThrowsForNullConfig() {
    // A null ServerConfiguration should throw an IllegalArgumentException.
    assertThrows(IllegalArgumentException.class, () -> builder.getAuthConfiguration(null));
  }

  @Test
  void getAuthConfigurationThrowsWhenAuthDisabled() {
    // A ServerConfiguration with auth disabled should throw.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(false);
    config.setAuth(authConfig);

    assertThrows(AssertionError.class, () -> builder.getAuthConfiguration(config));
  }

  @Test
  void getAuthConfigurationReturnsConfigWhenEnabled() {
    // A ServerConfiguration with auth enabled should return the auth configuration.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    config.setAuth(authConfig);

    final AuthorizationConfiguration result = builder.getAuthConfiguration(config);

    assertNotNull(result);
  }

  // -- build --

  @Test
  void buildCreatesDecoderWithIssuerAndAudience() {
    // The build method should produce a JwtDecoder when auth is enabled.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    authConfig.setIssuer("http://issuer.example.com");
    authConfig.setAudience("http://audience.example.com");
    config.setAuth(authConfig);

    final JwtDecoder decoder = builder.build(config);

    assertNotNull(decoder);
  }

  @Test
  void buildCreatesDecoderWithoutOptionalValidators() {
    // The build method should work even without issuer and audience.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    config.setAuth(authConfig);

    final JwtDecoder decoder = builder.build(config);

    assertNotNull(decoder);
  }

  // -- selectKeys --

  @Test
  void selectKeysThrowsForNullClaimsSet() {
    // A null claims set should throw an IllegalArgumentException.
    assertThrows(IllegalArgumentException.class, () -> builder.selectKeys(null, null, null));
  }

  // -- selectKeys: algorithms derived from the JWKS --

  @Test
  void derivesEcAlgorithmFromJwksWithEcKey() throws Exception {
    // An EC P-384 key published with alg ES384 means ES384 tokens must be verifiable.
    final ECKey key = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(key);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.ES384, "ec-1"), claims(), null);

    assertThat(keys).hasSize(1);
    assertThat(keys.get(0)).isInstanceOf(ECPublicKey.class);
  }

  @Test
  void derivesRsaAlgorithmFromJwksWithRsaKey() throws Exception {
    // Existing behaviour: an RSA key published with alg RS256 verifies RS256 tokens.
    final RSAKey key = generateRsaKey("rsa-1", JWSAlgorithm.RS256);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(key);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.RS256, "rsa-1"), claims(), null);

    assertThat(keys).hasSize(1);
    assertThat(keys.get(0)).isInstanceOf(RSAPublicKey.class);
  }

  @Test
  void rejectsAlgorithmAbsentFromJwks() throws Exception {
    // A JWKS publishing only an RSA key must not accept an ES384 token.
    final RSAKey key = generateRsaKey("rsa-1", JWSAlgorithm.RS256);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(key);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.ES384, "rsa-1"), claims(), null);

    assertThat(keys).isEmpty();
  }

  @Test
  void derivesAlgorithmsFromKeyTypeWhenAlgAbsent() throws Exception {
    // A key that omits alg still contributes the algorithms implied by its key type.
    final ECKey key = generateEcKey("ec-1", null);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(key);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.ES384, "ec-1"), claims(), null);

    assertThat(keys).hasSize(1);
    assertThat(keys.get(0)).isInstanceOf(ECPublicKey.class);
  }

  @Test
  void decoderVerifiesEs384SignedToken() throws Exception {
    // End-to-end: a real ES384 signature verifies through the built decoder.
    final ECKey key = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(key);
    final JwtDecoder decoder = decoderBuilder.build(authEnabledConfiguration());

    final Jwt jwt = decoder.decode(signToken(key, JWSAlgorithm.ES384));

    assertThat(jwt.getSubject()).isEqualTo("test-subject");
  }

  @Test
  void decoderVerifiesRs256SignedToken() throws Exception {
    // End-to-end regression guard: RS256 verification is unchanged.
    final RSAKey key = generateRsaKey("rsa-1", JWSAlgorithm.RS256);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(key);
    final JwtDecoder decoder = decoderBuilder.build(authEnabledConfiguration());

    final Jwt jwt = decoder.decode(signToken(key, JWSAlgorithm.RS256));

    assertThat(jwt.getSubject()).isEqualTo("test-subject");
  }

  @Test
  void reportsEmptyJwksAsAKeySourceFailure() {
    // A JWKS with no usable keys must fail as a key source problem, so that the request is
    // rejected with a 401 rather than reported as a server error.
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor();

    assertThrows(
        KeySourceException.class,
        () -> decoderBuilder.selectKeys(header(JWSAlgorithm.RS256, "rsa-1"), claims(), null));
  }

  // -- selectKeys: algorithms pinned by configuration --

  @Test
  void configuredAlgorithmsExcludeOthersPublishedByTheJwks() throws Exception {
    // Pinning RS256 must reject ES384 even though the JWKS publishes a matching EC key.
    final RSAKey rsaKey = generateRsaKey("rsa-1", JWSAlgorithm.RS256);
    final ECKey ecKey = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(List.of("RS256"), rsaKey, ecKey);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.ES384, "ec-1"), claims(), null);

    assertThat(keys).isEmpty();
  }

  @Test
  void configuredAlgorithmsAcceptListedAlgorithm() throws Exception {
    // Pinning RS256 must continue to accept RS256 tokens.
    final RSAKey rsaKey = generateRsaKey("rsa-1", JWSAlgorithm.RS256);
    final ECKey ecKey = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(List.of("RS256"), rsaKey, ecKey);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.RS256, "rsa-1"), claims(), null);

    assertThat(keys).hasSize(1);
    assertThat(keys.get(0)).isInstanceOf(RSAPublicKey.class);
  }

  @Test
  void configuredEcAlgorithmSelectsMatchingKey() throws Exception {
    // Pinning ES384 accepts an ES384 token backed by a published EC key.
    final ECKey key = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(List.of("ES384"), key);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.ES384, "ec-1"), claims(), null);

    assertThat(keys).hasSize(1);
    assertThat(keys.get(0)).isInstanceOf(ECPublicKey.class);
  }

  @Test
  void emptyConfiguredAlgorithmsFallBackToDerivation() throws Exception {
    // An explicitly empty list behaves identically to the property being unset.
    final ECKey key = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(List.of(), key);

    final List<? extends Key> keys =
        decoderBuilder.selectKeys(header(JWSAlgorithm.ES384, "ec-1"), claims(), null);

    assertThat(keys).hasSize(1);
  }

  @Test
  void decoderRejectsTokenSignedWithUnpinnedAlgorithm() throws Exception {
    // End-to-end: a validly signed ES384 token is rejected when only RS256 is pinned.
    final RSAKey rsaKey = generateRsaKey("rsa-1", JWSAlgorithm.RS256);
    final ECKey ecKey = generateEcKey("ec-1", JWSAlgorithm.ES384);
    final PathlingJwtDecoderBuilder decoderBuilder = builderFor(List.of("RS256"), rsaKey, ecKey);
    final JwtDecoder decoder = decoderBuilder.build(authEnabledConfiguration());
    final String token = signToken(ecKey, JWSAlgorithm.ES384);

    assertThrows(JwtException.class, () -> decoder.decode(token));
  }

  // -- Helpers --

  /**
   * Builds a decoder builder whose JWKS endpoint publishes the supplied keys, with no pinned
   * algorithms.
   *
   * @param keys the keys to publish
   * @return the builder
   */
  @Nonnull
  private static PathlingJwtDecoderBuilder builderFor(@Nonnull final JWK... keys) {
    return builderFor(List.of(), keys);
  }

  /**
   * Builds a decoder builder whose JWKS endpoint publishes the supplied keys, with the supplied
   * pinned algorithms.
   *
   * @param algorithms the value of the tokenSigningAlgorithms property
   * @param keys the keys to publish
   * @return the builder
   */
  @Nonnull
  private static PathlingJwtDecoderBuilder builderFor(
      @Nonnull final List<String> algorithms, @Nonnull final JWK... keys) {
    final OidcConfiguration oidcConfig =
        new OidcConfiguration(Map.of("jwks_uri", JwsTestFixtures.JWKS_URI));
    return new PathlingJwtDecoderBuilder(
        oidcConfig, configuration(algorithms), mockJwksTransport(jwksBody(keys)));
  }

  /**
   * Builds a server configuration with auth enabled and the supplied pinned algorithms.
   *
   * @param algorithms the value of the tokenSigningAlgorithms property
   * @return the configuration
   */
  @Nonnull
  private static ServerConfiguration configuration(@Nonnull final List<String> algorithms) {
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    authConfig.setTokenSigningAlgorithms(algorithms);
    config.setAuth(authConfig);
    return config;
  }

  /**
   * Builds a server configuration with auth enabled and the issuer and audience asserted by the
   * fixture tokens.
   *
   * @return the configuration
   */
  @Nonnull
  private static ServerConfiguration authEnabledConfiguration() {
    final ServerConfiguration config = configuration(List.of());
    config.getAuth().setIssuer("http://issuer.example.com");
    config.getAuth().setAudience("http://audience.example.com");
    return config;
  }
}

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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.JWKGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

/**
 * Test fixtures for exercising JWS signing algorithm selection: real key generation, real token
 * signing, and a mocked HTTP client that serves a JWKS document.
 *
 * @author John Grimes
 */
final class JwsTestFixtures {

  /** The JWKS URI used by tests, matching the one configured into the test OIDC configuration. */
  static final String JWKS_URI = "http://localhost/.well-known/jwks.json";

  private JwsTestFixtures() {}

  /**
   * Generates an RSA key pair suitable for signing.
   *
   * @param keyId the key identifier to assign
   * @param algorithm the algorithm to advertise in the key's {@code alg} field, or null to omit it
   * @return a new RSA JWK containing the private key
   * @throws JOSEException if key generation fails
   */
  @Nonnull
  static RSAKey generateRsaKey(@Nonnull final String keyId, @Nullable final JWSAlgorithm algorithm)
      throws JOSEException {
    final JWKGenerator<RSAKey> generator =
        new RSAKeyGenerator(2048).keyID(keyId).keyUse(KeyUse.SIGNATURE);
    if (algorithm != null) {
      generator.algorithm(algorithm);
    }
    return generator.generate();
  }

  /**
   * Generates an EC P-384 key pair suitable for signing with ES384.
   *
   * @param keyId the key identifier to assign
   * @param algorithm the algorithm to advertise in the key's {@code alg} field, or null to omit it
   * @return a new EC JWK containing the private key
   * @throws JOSEException if key generation fails
   */
  @Nonnull
  static ECKey generateEcKey(@Nonnull final String keyId, @Nullable final JWSAlgorithm algorithm)
      throws JOSEException {
    final JWKGenerator<ECKey> generator =
        new ECKeyGenerator(Curve.P_384).keyID(keyId).keyUse(KeyUse.SIGNATURE);
    if (algorithm != null) {
      generator.algorithm(algorithm);
    }
    return generator.generate();
  }

  /**
   * Renders the public halves of the supplied keys as a JWKS document.
   *
   * @param keys the keys to publish
   * @return the JSON representation of the JWKS
   */
  @Nonnull
  static String jwksBody(@Nonnull final JWK... keys) {
    final List<JWK> publicKeys = Arrays.stream(keys).map(JWK::toPublicJWK).toList();
    return new JWKSet(publicKeys).toString();
  }

  /**
   * Signs a JWT with the supplied key and algorithm.
   *
   * @param key the key to sign with, which must contain the private key material
   * @param algorithm the JWS algorithm to name in the header
   * @return the serialised, signed JWT
   * @throws JOSEException if signing fails
   */
  @Nonnull
  static String signToken(@Nonnull final JWK key, @Nonnull final JWSAlgorithm algorithm)
      throws JOSEException {
    final JWSSigner signer =
        key instanceof final ECKey ecKey ? new ECDSASigner(ecKey) : new RSASSASigner((RSAKey) key);
    final JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .issuer("http://issuer.example.com")
            .audience("http://audience.example.com")
            .expirationTime(new Date(System.currentTimeMillis() + 3600_000))
            .build();
    final SignedJWT jwt =
        new SignedJWT(new JWSHeader.Builder(algorithm).keyID(key.getKeyID()).build(), claims);
    jwt.sign(signer);
    return jwt.serialize();
  }

  /**
   * Builds a JWS header naming the supplied algorithm and key.
   *
   * @param algorithm the JWS algorithm
   * @param keyId the key identifier, or null to omit it
   * @return the header
   */
  @Nonnull
  static JWSHeader header(@Nonnull final JWSAlgorithm algorithm, @Nullable final String keyId) {
    final JWSHeader.Builder builder = new JWSHeader.Builder(algorithm);
    if (keyId != null) {
      builder.keyID(keyId);
    }
    return builder.build();
  }

  /**
   * Builds a minimal claims set for driving key selection.
   *
   * @return the claims set
   */
  @Nonnull
  static JWTClaimsSet claims() {
    return new JWTClaimsSet.Builder().subject("test-subject").build();
  }

  /**
   * Creates a mocked HTTP client that serves the supplied JWKS body for the test JWKS URI.
   *
   * @param body the JWKS document to return
   * @return the mocked client
   */
  @Nonnull
  static RestOperations mockJwksTransport(@Nonnull final String body) {
    final RestOperations restOperations = mock(RestOperations.class);
    when(restOperations.exchange(any(RequestEntity.class), eq(String.class)))
        .thenReturn(new ResponseEntity<>(body, HttpStatus.OK));
    return restOperations;
  }
}

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

package au.csiro.pathling.auth;

import static au.csiro.pathling.test.TestUtils.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.auth0.jwt.algorithms.Algorithm;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JWTUtils}.
 */
public class JWTUtilsTest {
  
  @Test
  void getsAlgorithmForRS384Key() throws Exception {
    final String privateKeyJWKJson = getResourceAsString("auth/bulk_rs384_priv_jwk.json");
    final JWK privateKeyJWK = JWK.parse(privateKeyJWKJson);
    final Algorithm algo = JWTUtils.getAsymmSigningAlgorithm(
        privateKeyJWK);
    assertEquals("RS384", algo.getName());
    assertNotNull(algo.sign("test".getBytes()));
  }
  
  @Test
  void getsAlgorithmForECS384Key() throws Exception {
    final String privateKeyJWKJson = getResourceAsString("auth/bulk_es384_priv_jwk.json");
    final JWK privateKeyJWK = JWK.parse(privateKeyJWKJson);
    final Algorithm algo = JWTUtils.getAsymmSigningAlgorithm(
        privateKeyJWK);
    assertEquals("ES384", algo.getName());
    assertNotNull(algo.sign("test".getBytes()));
  }

  @Test
  void failsForNonPrivateKey() throws Exception {
    final String privateKeyJWKSJson = getResourceAsString("auth/bulk_rs384_jwks.json");
    final JWKSet keySetJWKS = JWKSet.parse(privateKeyJWKSJson);
    final JWK publicKey = keySetJWKS.getKeys().get(0);
    assertFalse(publicKey.isPrivate());
    final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> JWTUtils.getAsymmSigningAlgorithm(publicKey));
    assertEquals("Only private keys are supported", ex.getMessage());
  }

  @Test
  void failsForSymmetricKey() throws Exception {

    final OctetSequenceKey key = new OctetSequenceKey.Builder("test".getBytes()).algorithm(
        new com.nimbusds.jose.Algorithm("HS384")).build();
    final JWK symmetricKeyJWK = JWK.parse(key.toJSONString());
    assertFalse(symmetricKeyJWK instanceof AsymmetricJWK);
    final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> JWTUtils.getAsymmSigningAlgorithm(symmetricKeyJWK));
    assertEquals("Only asymmetric keys are supported", ex.getMessage());
  }
}

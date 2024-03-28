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

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.auth0.jwt.JWT;
import com.auth0.jwt.RegisteredClaims;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import java.security.interfaces.ECKey;
import java.security.interfaces.RSAKey;
import java.text.ParseException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.http.message.BasicNameValuePair;
import org.junit.jupiter.api.Test;

class AsymmetricClientCredentialsTest {

  @Test
  void createsCorrectAssertionAndHeadersForRS384Key() throws JOSEException, ParseException {

    final String rsaKeyJWKSJson = getResourceAsString("auth/bulk_rs384_jwks.json");
    final JWKSet rsaKeySetJWKS = JWKSet.parse(rsaKeyJWKSJson);
    final JWK publicKey = rsaKeySetJWKS.getKeys().get(0);
    final JWK privateKey = rsaKeySetJWKS.getKeys().get(1);

    final AsymmetricClientCredentials credentials = AsymmetricClientCredentials.builder()
        .tokenEndpoint("token_endpoint_1")
        .clientId("client_id_1")
        .scope("scope_1")
        .privateKey(privateKey)
        .build();

    final Instant now = Instant.now();

    assertEquals(Collections.emptyList(), credentials.getAuthHeaders());
    final List<BasicNameValuePair> authAssertions = credentials.getAuthParams(now);
    assertEquals(2, authAssertions.size());
    assertEquals("urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        authAssertions.stream().filter(p -> p.getName().equals("client_assertion_type"))
            .findFirst().orElseThrow().getValue());
    final String clientAssertionJWT = authAssertions.stream()
        .filter(p -> p.getName().equals("client_assertion")).findFirst().orElseThrow().getValue();
    final DecodedJWT jwt = JWT.decode(clientAssertionJWT);
    final Algorithm algo = Algorithm.RSA384((RSAKey) ((AsymmetricJWK) publicKey).toPublicKey());
    // verify the token
    JWT.require(algo)
        .withIssuer("client_id_1")
        .withSubject("client_id_1")
        .withAudience("token_endpoint_1")
        .withClaimPresence(RegisteredClaims.JWT_ID)
        .withClaim(RegisteredClaims.EXPIRES_AT, now.getEpochSecond() + 60)
        .build().verify(jwt);
  }

  @Test
  void createsCorrectAssertionAndHeadersForES384Key() throws JOSEException, ParseException {

    final String esKeyJWKSJson = getResourceAsString("auth/bulk_es384_jwks.json");
    final JWKSet esKeySetJWKS = JWKSet.parse(esKeyJWKSJson);
    final JWK publicKey = esKeySetJWKS.getKeys().get(0);
    final JWK privateKey = esKeySetJWKS.getKeys().get(1);

    final AsymmetricClientCredentials credentials = AsymmetricClientCredentials.builder()
        .tokenEndpoint("token_endpoint_2")
        .clientId("client_id_2")
        .scope("scope_2")
        .privateKey(privateKey)
        .build();

    final Instant now = Instant.now();

    assertEquals(Collections.emptyList(), credentials.getAuthHeaders());
    final List<BasicNameValuePair> authAssertions = credentials.getAuthParams(now);
    assertEquals(2, authAssertions.size());
    assertEquals("urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        authAssertions.stream().filter(p -> p.getName().equals("client_assertion_type"))
            .findFirst().orElseThrow().getValue());
    final String clientAssertionJWT = authAssertions.stream()
        .filter(p -> p.getName().equals("client_assertion")).findFirst().orElseThrow().getValue();
    final DecodedJWT jwt = JWT.decode(clientAssertionJWT);
    final Algorithm algo = Algorithm.ECDSA384((ECKey) ((AsymmetricJWK) publicKey).toPublicKey());
    // verify the token
    JWT.require(algo)
        .withIssuer("client_id_2")
        .withSubject("client_id_2")
        .withAudience("token_endpoint_2")
        .withClaimPresence(RegisteredClaims.JWT_ID)
        .withClaim(RegisteredClaims.EXPIRES_AT, now.getEpochSecond() + 60)
        .build().verify(jwt);
  }
}

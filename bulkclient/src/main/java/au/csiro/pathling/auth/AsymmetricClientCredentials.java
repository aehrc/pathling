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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.nimbusds.jose.jwk.JWK;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.http.message.BasicNameValuePair;


@Value
public class AsymmetricClientCredentials implements ClientCredentials {

  @Nonnull
  String tokenEndpoint;

  @Nonnull
  String clientId;

  @Nonnull
  String privateKeyJWK;

  @Nullable
  String scope;

  @Nonnull
  @Override
  public List<BasicNameValuePair> getAssertions() {

    try {
      final JWK privateKey = JWK.parse(privateKeyJWK);
      final String kid = privateKey.getKeyID();
      final Algorithm algo = JWTUtils.getAsymmSigningAlgorithm(privateKey);
      final String jwt = JWT.create()
          .withHeader(Map.of("kid", kid))
          .withClaim("iss", clientId)
          .withClaim("sub", clientId)
          .withClaim("aud", tokenEndpoint)
          .withClaim("exp", Instant.now().plus(Duration.ofMinutes(3)).getEpochSecond())
          .withClaim("jti", UUID.randomUUID().toString())
          .sign(algo);

      return List.of(
          new BasicNameValuePair("client_assertion_type",
              "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"),
          new BasicNameValuePair("client_assertion", jwt)
      );
    } catch (final ParseException e) {
      throw new RuntimeException(e);
    }
  }
}

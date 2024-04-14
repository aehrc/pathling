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

import static com.auth0.jwt.RegisteredClaims.AUDIENCE;
import static com.auth0.jwt.RegisteredClaims.EXPIRES_AT;
import static com.auth0.jwt.RegisteredClaims.ISSUER;
import static com.auth0.jwt.RegisteredClaims.JWT_ID;
import static com.auth0.jwt.RegisteredClaims.SUBJECT;

import com.auth0.jwt.HeaderParams;
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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.http.message.BasicNameValuePair;


/**
 * The implementation of the SMART symmetric client authentication method.
 *
 * @see <a
 * href="https://www.hl7.org/fhir/smart-app-launch/client-confidential-asymmetric.html">Client
 * Authentication: Asymmetric</a>
 */
@Value
@EqualsAndHashCode(callSuper = false)
@Builder
public class AsymmetricClientAuthMethod extends ClientAuthMethod {

  /**
   * The client assertion type parameter.
   */
  static final String PARAM_CLIENT_ASSERTION_TYPE = "client_assertion_type";
  /**
   * The client assertion parameter.
   */
  static final String PARAM_CLIENT_ASSERTION = "client_assertion";

  /**
   * The grant type for refresh token.
   */
  static final String CLIENT_ASSERTION_TYPE_JWT_BEARER = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
  
  /**
   * The default expiry time for the JWT in seconds.
   */
  public static int DEFAULT_JWT_EXPIRY_IN_SECONDS = 60;

  @Nonnull
  String tokenEndpoint;

  @Nonnull
  String clientId;

  @Nonnull
  JWK privateKey;

  @Nullable
  @Builder.Default
  String scope = null;

  /**
   * The builder for the asymmetric client authentication method.
   */
  @SuppressWarnings("unused")
  public static class AsymmetricClientAuthMethodBuilder {

    /**
     * @param privateKeyJWK The private key in JWK format.
     * @return The builder.
     */
    @Nonnull
    public AsymmetricClientAuthMethodBuilder privateKeyJWK(@Nonnull final String privateKeyJWK) {
      try {
        this.privateKey = JWK.parse(privateKeyJWK);
      } catch (final ParseException ex) {
        throw new IllegalArgumentException("Invalid JWK: " + ex.getMessage(), ex);
      }
      return this;
    }
  }

  @Nonnull
  @Override
  protected List<BasicNameValuePair> getAuthParams(@Nonnull final Instant now) {
    final String kid = privateKey.getKeyID();
    final Algorithm algo = JWTUtils.getAsymmSigningAlgorithm(privateKey);
    final String jwt = JWT.create()
        .withHeader(Map.of(HeaderParams.KEY_ID, kid))
        .withClaim(ISSUER, clientId)
        .withClaim(SUBJECT, clientId)
        .withClaim(AUDIENCE, tokenEndpoint)
        .withClaim(EXPIRES_AT,
            now.plus(Duration.ofSeconds(DEFAULT_JWT_EXPIRY_IN_SECONDS)).getEpochSecond())
        .withClaim(JWT_ID, UUID.randomUUID().toString())
        .sign(algo);

    return List.of(
        new BasicNameValuePair(PARAM_CLIENT_ASSERTION_TYPE,
            CLIENT_ASSERTION_TYPE_JWT_BEARER),
        new BasicNameValuePair(PARAM_CLIENT_ASSERTION, jwt)
    );
  }
}

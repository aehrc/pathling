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

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Audience validator for JWTs.
 *
 * @see <a
 *     href="https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization">Spring
 *     Security 5 Java API: Authorization</a>
 */
public class JwtAudienceValidator implements OAuth2TokenValidator<Jwt> {

  private final String audience;

  /**
   * @param audience the required audience value to be presented within tokens
   */
  public JwtAudienceValidator(final String audience) {
    this.audience = audience;
  }

  @Override
  public OAuth2TokenValidatorResult validate(final Jwt jwt) {
    final OAuth2Error error =
        new OAuth2Error("invalid_token", "The required audience is missing", null);

    if (jwt.getAudience().contains(audience)) {
      return OAuth2TokenValidatorResult.success();
    }
    return OAuth2TokenValidatorResult.failure(error);
  }
}

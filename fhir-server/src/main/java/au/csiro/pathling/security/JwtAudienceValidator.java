/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Audience validator for JWTs.
 *
 * @see <a href="https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization">Spring
 * Security 5 Java API: Authorization</a>
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
    final OAuth2Error error = new OAuth2Error("invalid_token", "The required audience is missing",
        null);

    if (jwt.getAudience().contains(audience)) {
      return OAuth2TokenValidatorResult.success();
    }
    return OAuth2TokenValidatorResult.failure(error);
  }

}

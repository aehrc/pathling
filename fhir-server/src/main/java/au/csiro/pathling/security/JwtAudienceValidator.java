package au.csiro.pathling.security;

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Audience validator for Jwt tokens.
 * Based on code from: https://auth0.com/docs/quickstart/backend/java-spring-security5/01-authorization
 */
class JwtAudienceValidator implements OAuth2TokenValidator<Jwt> {

  private final String audience;

  JwtAudienceValidator(String audience) {
    this.audience = audience;
  }

  @Override
  public OAuth2TokenValidatorResult validate(Jwt jwt) {
    OAuth2Error error = new OAuth2Error("invalid_token", "The required audience is missing", null);

    if (jwt.getAudience().contains(audience)) {
      return OAuth2TokenValidatorResult.success();
    }
    return OAuth2TokenValidatorResult.failure(error);
  }
}
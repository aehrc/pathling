/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class JwtAudienceValidatorTest {

  @Test
  void validateSuccess() {
    final OAuth2TokenValidator<Jwt> audienceValidator = new JwtAudienceValidator("foo");
    final Jwt jwt = mock(Jwt.class);
    when(jwt.getAudience()).thenReturn(Collections.singletonList("foo"));
    final OAuth2TokenValidatorResult result = audienceValidator.validate(jwt);
    Assertions.assertFalse(result.hasErrors());
  }

  @Test
  void validateFailure() {
    final OAuth2TokenValidator<Jwt> audienceValidator = new JwtAudienceValidator("foo");
    final Jwt jwt = mock(Jwt.class);
    when(jwt.getAudience()).thenReturn(Collections.singletonList("bar"));
    final OAuth2TokenValidatorResult result = audienceValidator.validate(jwt);
    Assertions.assertTrue(result.hasErrors());
  }

}

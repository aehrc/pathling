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

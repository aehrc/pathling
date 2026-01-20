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

package au.csiro.pathling.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuthorizationConfiguration} validation.
 *
 * @author John Grimes
 */
class AuthorizationConfigurationTest {

  private Validator validator;

  @BeforeEach
  void setUp() {
    validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  // -------------------------------------------------------------------------
  // Code challenge methods validation tests
  // -------------------------------------------------------------------------

  @Test
  void acceptsValidCodeChallengeMethods() {
    // S256 is included, which satisfies the SMART spec requirement.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setCodeChallengeMethodsSupported(List.of("S256"));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).isEmpty();
  }

  @Test
  void acceptsMultipleValidCodeChallengeMethods() {
    // S256 plus additional methods is valid.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setCodeChallengeMethodsSupported(List.of("S256", "S384", "S512"));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).isEmpty();
  }

  @Test
  void rejectsCodeChallengeMethodsWithoutS256() {
    // S256 is required by SMART spec.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setCodeChallengeMethodsSupported(List.of("S384"));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getMessage())
        .isEqualTo("codeChallengeMethodsSupported must include 'S256'");
  }

  @Test
  void rejectsCodeChallengeMethodsWithPlain() {
    // plain is prohibited by SMART spec.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setCodeChallengeMethodsSupported(List.of("S256", "plain"));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getMessage())
        .isEqualTo("codeChallengeMethodsSupported must not include 'plain'");
  }

  @Test
  void rejectsCodeChallengeMethodsWithOnlyPlain() {
    // Both violations: missing S256 and has plain.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setCodeChallengeMethodsSupported(List.of("plain"));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    // Should have two violations: missing S256 and has plain.
    assertThat(violations).hasSize(2);
  }

  // -------------------------------------------------------------------------
  // Default values tests
  // -------------------------------------------------------------------------

  @Test
  void hasCorrectDefaultGrantTypes() {
    // Default should be authorization_code for backward compatibility.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();

    assertThat(config.getGrantTypesSupported()).containsExactly("authorization_code");
  }

  @Test
  void hasCorrectDefaultCodeChallengeMethods() {
    // Default should be S256 to satisfy SMART spec requirement.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();

    assertThat(config.getCodeChallengeMethodsSupported()).containsExactly("S256");
  }
}

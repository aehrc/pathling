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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

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
  // Token signing algorithm validation tests
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {"HS256", "HS384", "HS512", "none", "RS999", "rs256", ""})
  void rejectsNonAsymmetricTokenSigningAlgorithm(final String algorithm) {
    // Verification runs against a public JWKS, so only asymmetric algorithm names are usable.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setTokenSigningAlgorithms(List.of(algorithm));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).hasSize(1);
    final ConstraintViolation<AuthorizationConfiguration> violation = violations.iterator().next();
    assertThat(violation.getPropertyPath()).hasToString("tokenSigningAlgorithms[0].<list element>");
    assertThat(violation.getInvalidValue()).isEqualTo(algorithm);
    assertThat(violation.getMessage())
        .isEqualTo(
            "tokenSigningAlgorithms must contain only asymmetric JWS algorithms: RS256, RS384, "
                + "RS512, PS256, PS384, PS512, ES256, ES256K, ES384, ES512, EdDSA");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES256K", "ES384", "ES512",
        "EdDSA"
      })
  void acceptsAsymmetricTokenSigningAlgorithm(final String algorithm) {
    // Every asymmetric JWS algorithm name is accepted.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setTokenSigningAlgorithms(List.of(algorithm));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).isEmpty();
  }

  @Test
  void reportsTheIndexOfEachOffendingAlgorithm() {
    // The violation must identify which element of the list was rejected.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setTokenSigningAlgorithms(List.of("RS256", "HS256"));

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().getPropertyPath())
        .hasToString("tokenSigningAlgorithms[1].<list element>");
  }

  @Test
  void bindingFailsForNonAsymmetricTokenSigningAlgorithm() {
    // Binding is the mechanism Spring Boot uses to populate configuration properties at startup, so
    // a rejected value here is what prevents the server from starting.
    final MapConfigurationPropertySource source =
        new MapConfigurationPropertySource(
            Map.of(
                "pathling.auth.enabled", "true",
                "pathling.auth.tokenSigningAlgorithms[0]", "HS256"));
    final BindHandler handler =
        new ValidationBindHandler(
            new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator()));

    assertThatThrownBy(
            () ->
                new Binder(source)
                    .bind("pathling.auth", Bindable.of(AuthorizationConfiguration.class), handler))
        .isInstanceOf(BindException.class)
        .rootCause()
        .isInstanceOf(BindValidationException.class)
        .hasMessageContaining("tokenSigningAlgorithms[0]")
        .hasMessageContaining("HS256")
        .hasMessageContaining("asymmetric JWS algorithms");
  }

  @Test
  void bindingSucceedsForAsymmetricTokenSigningAlgorithm() {
    // The same binding path accepts a valid asymmetric algorithm name.
    final MapConfigurationPropertySource source =
        new MapConfigurationPropertySource(
            Map.of(
                "pathling.auth.enabled", "true",
                "pathling.auth.tokenSigningAlgorithms[0]", "ES384"));
    final BindHandler handler =
        new ValidationBindHandler(
            new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator()));

    final AuthorizationConfiguration bound =
        new Binder(source)
            .bind("pathling.auth", Bindable.of(AuthorizationConfiguration.class), handler)
            .get();

    assertThat(bound.getTokenSigningAlgorithms()).containsExactly("ES384");
  }

  @Test
  void acceptsEmptyTokenSigningAlgorithms() {
    // An empty list is valid, and means the algorithms are derived from the issuer's JWKS.
    final AuthorizationConfiguration config = new AuthorizationConfiguration();
    config.setTokenSigningAlgorithms(List.of());

    final Set<ConstraintViolation<AuthorizationConfiguration>> violations =
        validator.validate(config);

    assertThat(violations).isEmpty();
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

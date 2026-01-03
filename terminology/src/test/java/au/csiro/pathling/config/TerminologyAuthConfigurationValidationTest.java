/*
 * Copyright 2018-2025 Commonwealth Scientific and Industrial Research
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.TerminologyAuthConfiguration.TerminologyAuthConfigValidator;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TerminologyAuthConfigValidator}.
 *
 * @author John Grimes
 */
class TerminologyAuthConfigurationValidationTest {

  private final TerminologyAuthConfigValidator validator = new TerminologyAuthConfigValidator();

  @Test
  void validWhenDisabled() {
    // When authentication is disabled, no other fields are required.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder().enabled(false).build();

    assertTrue(validator.isValid(config, null));
  }

  @Test
  void validWhenEnabledWithClientSecret() {
    // When enabled with all required fields and client secret.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder()
            .enabled(true)
            .tokenEndpoint("https://auth.example.com/token")
            .clientId("my-client")
            .clientSecret("my-secret")
            .build();

    assertTrue(validator.isValid(config, null));
  }

  @Test
  void validWhenEnabledWithPrivateKeyJwk() {
    // When enabled with all required fields and private key JWK.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder()
            .enabled(true)
            .tokenEndpoint("https://auth.example.com/token")
            .clientId("my-client")
            .privateKeyJWK("{\"kty\":\"RSA\",\"n\":\"test\"}")
            .build();

    assertTrue(validator.isValid(config, null));
  }

  @Test
  void invalidWhenEnabledWithoutTokenEndpoint() {
    // When enabled but missing token endpoint.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder()
            .enabled(true)
            .clientId("my-client")
            .clientSecret("my-secret")
            .build();

    assertFalse(validator.isValid(config, null));
  }

  @Test
  void invalidWhenEnabledWithoutClientId() {
    // When enabled but missing client ID.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder()
            .enabled(true)
            .tokenEndpoint("https://auth.example.com/token")
            .clientSecret("my-secret")
            .build();

    assertFalse(validator.isValid(config, null));
  }

  @Test
  void invalidWhenEnabledWithoutCredentials() {
    // When enabled but missing both client secret and private key.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder()
            .enabled(true)
            .tokenEndpoint("https://auth.example.com/token")
            .clientId("my-client")
            .build();

    assertFalse(validator.isValid(config, null));
  }

  @Test
  void validWithBothSecretAndPrivateKey() {
    // When both client secret and private key are provided.
    final TerminologyAuthConfiguration config =
        TerminologyAuthConfiguration.builder()
            .enabled(true)
            .tokenEndpoint("https://auth.example.com/token")
            .clientId("my-client")
            .clientSecret("my-secret")
            .privateKeyJWK("{\"kty\":\"RSA\"}")
            .build();

    assertTrue(validator.isValid(config, null));
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.fhir.auth.AuthConfig;
import au.csiro.pathling.operations.bulksubmit.SubmitterIdentifier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SubmitterConfiguration} covering toIdentifier, hasCredentials, and toAuthConfig.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class SubmitterConfigurationTest {

  // -- toIdentifier --

  @Test
  void toIdentifierCreatesSubmitterIdentifier() {
    // The toIdentifier method should create a SubmitterIdentifier from system and value.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", null, null, null, null, null, null);
    final SubmitterIdentifier identifier = config.toIdentifier();
    assertEquals("http://system.org", identifier.system());
    assertEquals("sub-1", identifier.value());
  }

  // -- hasCredentials --

  @Test
  void hasCredentialsReturnsTrueWithClientIdAndSecret() {
    // A configuration with clientId and clientSecret should have credentials.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", "client-id", "client-secret", null, null, null, null);
    assertTrue(config.hasCredentials());
  }

  @Test
  void hasCredentialsReturnsTrueWithClientIdAndPrivateKey() {
    // A configuration with clientId and privateKeyJwk should have credentials.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", "client-id", null, "{\"kty\":\"RSA\"}", null, null, null);
    assertTrue(config.hasCredentials());
  }

  @Test
  void hasCredentialsReturnsFalseWithoutClientId() {
    // A configuration without a clientId should not have credentials.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", null, "client-secret", null, null, null, null);
    assertFalse(config.hasCredentials());
  }

  @Test
  void hasCredentialsReturnsFalseWithClientIdOnly() {
    // A configuration with only a clientId (no secret or key) should not have credentials.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", "client-id", null, null, null, null, null);
    assertFalse(config.hasCredentials());
  }

  // -- toAuthConfig --

  @Test
  void toAuthConfigSetsAllFields() {
    // The toAuthConfig method should populate all fields from the configuration.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org",
            "sub-1",
            "client-id",
            "client-secret",
            "{\"kty\":\"RSA\"}",
            "openid profile",
            60L,
            false);

    final AuthConfig authConfig = config.toAuthConfig("http://token.example.com");

    assertTrue(authConfig.isEnabled());
    assertFalse(authConfig.isUseSMART());
    assertEquals("http://token.example.com", authConfig.getTokenEndpoint());
    assertEquals("client-id", authConfig.getClientId());
    assertEquals("client-secret", authConfig.getClientSecret());
    assertEquals("{\"kty\":\"RSA\"}", authConfig.getPrivateKeyJWK());
    assertEquals("openid profile", authConfig.getScope());
    assertEquals(60L, authConfig.getTokenExpiryTolerance());
    assertFalse(authConfig.isUseFormForBasicAuth());
  }

  @Test
  void toAuthConfigUsesDefaultTokenExpiryTolerance() {
    // When tokenExpiryTolerance is null, the default (120 seconds) should be used.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", "client-id", "secret", null, null, null, null);

    final AuthConfig authConfig = config.toAuthConfig("http://token.example.com");

    assertEquals(
        SubmitterConfiguration.DEFAULT_TOKEN_EXPIRY_TOLERANCE,
        authConfig.getTokenExpiryTolerance());
  }

  @Test
  void toAuthConfigDefaultsUseFormForBasicAuthToTrue() {
    // When useFormForBasicAuth is null, it should default to true.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", "client-id", "secret", null, null, null, null);

    final AuthConfig authConfig = config.toAuthConfig("http://token.example.com");

    assertTrue(authConfig.isUseFormForBasicAuth());
  }

  @Test
  void toAuthConfigWithNullOptionalFields() {
    // The toAuthConfig method should handle null optional fields gracefully.
    final SubmitterConfiguration config =
        new SubmitterConfiguration(
            "http://system.org", "sub-1", null, null, null, null, null, null);

    final AuthConfig authConfig = config.toAuthConfig("http://token.example.com");

    assertNotNull(authConfig);
    assertNull(authConfig.getClientId());
    assertNull(authConfig.getClientSecret());
    assertNull(authConfig.getPrivateKeyJWK());
    assertNull(authConfig.getScope());
  }
}

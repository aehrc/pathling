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

package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * Tests for {@link PathlingJwtDecoderBuilder} covering auth configuration validation, decoder
 * building, and key selection error handling.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class PathlingJwtDecoderBuilderTest {

  private PathlingJwtDecoderBuilder builder;

  @BeforeEach
  void setUp() {
    // Create an OidcConfiguration with a test JWKS URI.
    final OidcConfiguration oidcConfig =
        new OidcConfiguration(Map.of("jwks_uri", "http://localhost/.well-known/jwks.json"));
    builder = new PathlingJwtDecoderBuilder(oidcConfig);
  }

  // -- getAuthConfiguration --

  @Test
  void getAuthConfigurationThrowsForNullConfig() {
    // A null ServerConfiguration should throw an IllegalArgumentException.
    assertThrows(IllegalArgumentException.class, () -> builder.getAuthConfiguration(null));
  }

  @Test
  void getAuthConfigurationThrowsWhenAuthDisabled() {
    // A ServerConfiguration with auth disabled should throw.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(false);
    config.setAuth(authConfig);

    assertThrows(AssertionError.class, () -> builder.getAuthConfiguration(config));
  }

  @Test
  void getAuthConfigurationReturnsConfigWhenEnabled() {
    // A ServerConfiguration with auth enabled should return the auth configuration.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    config.setAuth(authConfig);

    final AuthorizationConfiguration result = builder.getAuthConfiguration(config);

    assertNotNull(result);
  }

  // -- build --

  @Test
  void buildCreatesDecoderWithIssuerAndAudience() {
    // The build method should produce a JwtDecoder when auth is enabled.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    authConfig.setIssuer("http://issuer.example.com");
    authConfig.setAudience("http://audience.example.com");
    config.setAuth(authConfig);

    final JwtDecoder decoder = builder.build(config);

    assertNotNull(decoder);
  }

  @Test
  void buildCreatesDecoderWithoutOptionalValidators() {
    // The build method should work even without issuer and audience.
    final ServerConfiguration config = new ServerConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    config.setAuth(authConfig);

    final JwtDecoder decoder = builder.build(config);

    assertNotNull(decoder);
  }

  // -- selectKeys --

  @Test
  void selectKeysThrowsForNullClaimsSet() {
    // A null claims set should throw an IllegalArgumentException.
    assertThrows(IllegalArgumentException.class, () -> builder.selectKeys(null, null, null));
  }
}

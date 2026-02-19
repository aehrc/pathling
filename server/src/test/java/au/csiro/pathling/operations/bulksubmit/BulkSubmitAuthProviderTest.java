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

package au.csiro.pathling.operations.bulksubmit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.ServerConfiguration;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BulkSubmitAuthProvider} covering token acquisition when no config is found, when
 * credentials are not configured, cache clearing, and resource cleanup.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class BulkSubmitAuthProviderTest {

  private BulkSubmitAuthProvider authProvider;

  @BeforeEach
  void setUp() {
    // Create a provider with default (empty) configuration.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    authProvider = new BulkSubmitAuthProvider(serverConfig);
  }

  @AfterEach
  void tearDown() throws IOException {
    authProvider.close();
  }

  @Test
  void acquireTokenReturnsEmptyWhenNoBulkSubmitConfig() throws IOException {
    // When there is no bulk submit configuration, acquireToken should return empty.
    final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "sub-1");

    final Optional<String> token =
        authProvider.acquireToken(submitter, "http://localhost/fhir", null);

    assertTrue(token.isEmpty());
  }

  @Test
  void acquireTokenReturnsEmptyWhenSubmitterNotConfigured() throws IOException {
    // When the submitter is not in the allowed submitters list, acquireToken should return empty.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    final au.csiro.pathling.config.BulkSubmitConfiguration bulkSubmitConfig =
        new au.csiro.pathling.config.BulkSubmitConfiguration();
    serverConfig.setBulkSubmit(bulkSubmitConfig);

    try (final BulkSubmitAuthProvider provider = new BulkSubmitAuthProvider(serverConfig)) {
      final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "unknown");

      final Optional<String> token =
          provider.acquireToken(submitter, "http://localhost/fhir", null);

      assertTrue(token.isEmpty());
    }
  }

  @Test
  void acquireTokenReturnsEmptyWhenNoCredentials() throws IOException {
    // When the submitter has no credentials configured, acquireToken should return empty.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    final au.csiro.pathling.config.BulkSubmitConfiguration bulkSubmitConfig =
        new au.csiro.pathling.config.BulkSubmitConfiguration();
    final au.csiro.pathling.config.SubmitterConfiguration submitterConfig =
        new au.csiro.pathling.config.SubmitterConfiguration(
            "http://system.org", "sub-1", null, null, null, null, null, null);
    bulkSubmitConfig.getAllowedSubmitters().add(submitterConfig);
    serverConfig.setBulkSubmit(bulkSubmitConfig);

    try (final BulkSubmitAuthProvider provider = new BulkSubmitAuthProvider(serverConfig)) {
      final SubmitterIdentifier submitter = new SubmitterIdentifier("http://system.org", "sub-1");

      final Optional<String> token =
          provider.acquireToken(submitter, "http://localhost/fhir", null);

      assertTrue(token.isEmpty());
    }
  }

  @Test
  void clearTokenCacheDoesNotThrow() {
    // Clearing the token cache on a fresh provider should not throw.
    assertDoesNotThrow(() -> authProvider.clearTokenCache());
  }

  @Test
  void closeReleasesResources() {
    // Closing the provider should not throw.
    assertDoesNotThrow(() -> authProvider.close());
  }
}

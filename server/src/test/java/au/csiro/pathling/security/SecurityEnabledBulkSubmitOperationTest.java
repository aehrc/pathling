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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.bulksubmit.BulkSubmitExecutor;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Security tests for the bulk submit operation. Verifies that resource-level authorisation is
 * enforced in addition to operation-level authority.
 *
 * @author John Grimes
 */
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir"
    })
@MockitoBean(types = OidcConfiguration.class)
@MockitoBean(types = JwtDecoder.class)
@MockitoBean(types = JwtAuthenticationConverter.class)
@Import(FhirServerTestConfiguration.class)
class SecurityEnabledBulkSubmitOperationTest extends SecurityTest {

  private static final String ERROR_MSG_TEMPLATE = "Missing authority: 'pathling:%s'";

  @Autowired private BulkSubmitExecutor bulkSubmitExecutor;

  // -------------------------------------------------------------------------
  // checkResourceWriteAuthority tests
  // -------------------------------------------------------------------------

  @Test
  @DisplayName("checkResourceWriteAuthority: User with bulk-submit and write authority → succeeds")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:bulk-submit", "pathling:write"})
  void checkResourceWriteAuthoritySucceedsWithGlobalWriteAuthority() {
    // Given: resource types that would be imported.
    final Map<String, Collection<String>> resourceTypes =
        Map.of("Patient", Set.of(), "Observation", Set.of());

    // When/Then: should succeed with global write authority.
    assertThatNoException()
        .isThrownBy(() -> bulkSubmitExecutor.checkResourceWriteAuthority(resourceTypes.keySet()));
  }

  @Test
  @DisplayName(
      "checkResourceWriteAuthority: User with bulk-submit and specific write authorities →"
          + " succeeds")
  @WithMockJwt(
      username = "user",
      authorities = {
        "pathling:bulk-submit",
        "pathling:write:Patient",
        "pathling:write:Observation"
      })
  void checkResourceWriteAuthoritySucceedsWithSpecificWriteAuthorities() {
    // Given: resource types that match the granted authorities.
    final Map<String, Collection<String>> resourceTypes =
        Map.of("Patient", Set.of(), "Observation", Set.of());

    // When/Then: should succeed with matching specific write authorities.
    assertThatNoException()
        .isThrownBy(() -> bulkSubmitExecutor.checkResourceWriteAuthority(resourceTypes.keySet()));
  }

  @Test
  @DisplayName(
      "checkResourceWriteAuthority: User with bulk-submit but no write authority →"
          + " AccessDeniedError")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:bulk-submit"})
  void checkResourceWriteAuthorityDeniedWithoutWriteAuthority() {
    // Given: resource types to be imported.
    final Map<String, Collection<String>> resourceTypes = Map.of("Patient", Set.of());

    // When/Then: should fail due to missing write authority.
    assertThatThrownBy(() -> bulkSubmitExecutor.checkResourceWriteAuthority(resourceTypes.keySet()))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("write:Patient"));
  }

  @Test
  @DisplayName(
      "checkResourceWriteAuthority: User with bulk-submit and partial write authority →"
          + " AccessDeniedError")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:bulk-submit", "pathling:write:Patient"})
  void checkResourceWriteAuthorityDeniedWithPartialWriteAuthority() {
    // Given: resource types including one not authorised.
    final Map<String, Collection<String>> resourceTypes =
        Map.of("Patient", Set.of(), "Observation", Set.of());

    // When/Then: should fail due to missing write authority for Observation.
    assertThatThrownBy(() -> bulkSubmitExecutor.checkResourceWriteAuthority(resourceTypes.keySet()))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("write:Observation"));
  }

  @Test
  @DisplayName("checkResourceWriteAuthority: User with pathling (super) authority → succeeds")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling"})
  void checkResourceWriteAuthoritySucceedsWithSuperAuthority() {
    // Given: resource types to be imported.
    final Map<String, Collection<String>> resourceTypes =
        Map.of("Patient", Set.of(), "Observation", Set.of(), "Condition", Set.of());

    // When/Then: should succeed with the super authority.
    assertThatNoException()
        .isThrownBy(() -> bulkSubmitExecutor.checkResourceWriteAuthority(resourceTypes.keySet()));
  }

  @Test
  @DisplayName("checkResourceWriteAuthority: Empty resource types → succeeds")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:bulk-submit"})
  void checkResourceWriteAuthoritySucceedsWithEmptyResourceTypes() {
    // Given: no resource types to check.
    final Set<String> resourceTypes = Set.of();

    // When/Then: should succeed with empty resource types.
    assertThatNoException()
        .isThrownBy(() -> bulkSubmitExecutor.checkResourceWriteAuthority(resourceTypes));
  }
}

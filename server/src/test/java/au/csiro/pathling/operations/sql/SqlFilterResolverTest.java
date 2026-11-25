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

package au.csiro.pathling.operations.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Tests for the read authority checks that {@link SqlFilterResolver} applies to the {@code patient}
 * and {@code group} filters before resolving them.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class SqlFilterResolverTest {

  /** The message the data source raises when no table exists for a type. */
  private static final String NO_DATA_MESSAGE = "No data found for resource type: Patient";

  @Mock private QueryableDataSource deltaLake;
  @Mock private GroupMemberService groupMemberService;

  private AuthorizationConfiguration authConfig;

  private SqlFilterResolver resolver;

  @BeforeEach
  void setUp() {
    authConfig = new AuthorizationConfiguration();
    final ServerConfiguration configuration = new ServerConfiguration();
    configuration.setAuth(authConfig);
    resolver = new SqlFilterResolver(deltaLake, groupMemberService, configuration);
  }

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
  }

  // ---------------------------------------------------------------------------
  // The patient filter.
  // ---------------------------------------------------------------------------

  // Checking whether a named patient exists reveals Patient data, so a caller without read access
  // to Patient is refused before the Patient table is consulted.
  @Test
  void patientFilterRejectsCallerWithoutPatientReadAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:sql-run", "pathling:read:Observation");

    assertThatThrownBy(() -> resolver.resolve(List.of(new Reference("Patient/p1")), null, null))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:read:Patient");
    verifyNoInteractions(deltaLake);
  }

  // With read access to Patient the existence check runs, and an id that names no stored Patient
  // is reported as before.
  @Test
  void patientFilterIsResolvedWithPatientReadAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:sql-run", "pathling:read:Patient");
    when(deltaLake.read("Patient")).thenThrow(new IllegalArgumentException(NO_DATA_MESSAGE));

    final ResolvedFilters filters =
        resolver.resolve(List.of(new Reference("Patient/p1")), null, null);

    verify(deltaLake).read("Patient");
    assertThat(filters.patientIds()).isEmpty();
    assertThat(filters.issues())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.getCode()).isEqualTo(IssueType.NOTFOUND);
              assertThat(issue.getExpression().get(0).getValue())
                  .isEqualTo(SqlFilterResolver.PATIENT_EXPRESSION);
            });
  }

  // ---------------------------------------------------------------------------
  // The group filter.
  // ---------------------------------------------------------------------------

  // Resolving a group reveals whether it exists and who its members are, so a caller without read
  // access to Group is refused before the group is resolved, even when it can read Patient.
  @Test
  void groupFilterRejectsCallerWithoutGroupReadAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:sql-run", "pathling:read:Patient");

    assertThatThrownBy(() -> resolver.resolve(null, List.of(new Reference("Group/g1")), null))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:read:Group");
    verifyNoInteractions(groupMemberService);
  }

  // A group that does not exist must be refused in the same way, so that a 400 naming the missing
  // group cannot be used to probe which groups exist.
  @Test
  void groupFilterRejectsCallerWithoutGroupReadAuthorityForAnyGroupValue() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:sql-run", "pathling:read:Patient");

    assertThatThrownBy(
            () ->
                resolver.resolve(
                    null, List.of(new Reference("Group/nope"), new Reference("Group/g1")), null))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:read:Group");
    verifyNoInteractions(groupMemberService);
  }

  // With read access to Group the group resolves to its members.
  @Test
  void groupFilterIsResolvedWithGroupReadAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:sql-run", "pathling:read:Group");
    when(groupMemberService.extractPatientIdsFromGroup("g1")).thenReturn(Set.of("p1", "p2"));

    final ResolvedFilters filters =
        resolver.resolve(null, List.of(new Reference("Group/g1")), null);

    assertThat(filters.patientIds()).containsExactlyInAnyOrder("p1", "p2");
    assertThat(filters.issues()).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Requests that need no read authority.
  // ---------------------------------------------------------------------------

  // A request that supplies neither filter reads neither type, so it needs neither authority.
  @Test
  void absentFiltersRequireNoReadAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:sql-run");
    final InstantType since = new InstantType("2020-01-01T00:00:00Z");

    final ResolvedFilters filters = resolver.resolve(null, List.of(), since);

    assertThat(filters.patientIds()).isEmpty();
    assertThat(filters.since()).isSameAs(since);
    assertThat(filters.issues()).isEmpty();
    verifyNoInteractions(deltaLake, groupMemberService);
  }

  // With authorisation disabled there is no token, and both filters resolve as before.
  @Test
  void filtersAreResolvedWhenAuthDisabled() {
    authConfig.setEnabled(false);
    when(deltaLake.read("Patient")).thenThrow(new IllegalArgumentException(NO_DATA_MESSAGE));
    when(groupMemberService.extractPatientIdsFromGroup("g1")).thenReturn(Set.of("p2"));

    final ResolvedFilters filters =
        resolver.resolve(
            List.of(new Reference("Patient/p1")), List.of(new Reference("Group/g1")), null);

    assertThat(filters.patientIds()).containsExactly("p2");
    assertThat(filters.issues()).hasSize(1);
  }

  private static void setSecurityContext(final String... authorities) {
    final Jwt jwt = Jwt.withTokenValue("mock").header("alg", "none").claim("sub", "user").build();
    final JwtAuthenticationToken auth =
        new JwtAuthenticationToken(jwt, AuthorityUtils.createAuthorityList(authorities));
    SecurityContextHolder.getContext().setAuthentication(auth);
  }
}

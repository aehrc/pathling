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

package au.csiro.pathling.operations.bulkexport;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.r4.model.Group;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Tests for {@link GroupExportProvider} covering resource type, pre-async validation, group member
 * extraction, and delegation to the helper.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class GroupExportProviderTest {

  @Mock private ExportOperationValidator exportOperationValidator;
  @Mock private ExportOperationHelper exportOperationHelper;
  @Mock private QueryableDataSource deltaLake;
  @Mock private GroupMemberService groupMemberService;

  private AuthorizationConfiguration authConfig;

  private GroupExportProvider provider;

  @BeforeEach
  void setUp() {
    authConfig = new AuthorizationConfiguration();
    final ServerConfiguration configuration = new ServerConfiguration();
    configuration.setAuth(authConfig);
    provider =
        new GroupExportProvider(
            exportOperationValidator,
            exportOperationHelper,
            deltaLake,
            groupMemberService,
            configuration);
  }

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void getResourceTypeReturnsGroup() {
    // The provider should return Group as its resource type.
    assertEquals(Group.class, provider.getResourceType());
  }

  @Test
  void exportGroupDelegatesToHelper() {
    // The exportGroup method should delegate to the helper.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final Parameters expected = new Parameters();
    when(exportOperationHelper.executeExport(requestDetails)).thenReturn(expected);

    final Parameters result =
        provider.exportGroup(
            new IdType("Group/g1"), null, null, null, null, null, null, requestDetails);

    assertEquals(expected, result);
  }

  @Test
  void preAsyncValidateExtractsPatientIdsFromGroup() {
    // Pre-async validation should extract patient IDs from the group.
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final IdType groupId = new IdType("Group/g1");
    final Set<String> patientIds = Set.of("p1", "p2");

    when(groupMemberService.extractPatientIdsFromGroup("g1")).thenReturn(patientIds);

    final ExportRequest exportRequest =
        new ExportRequest(
            "http://localhost/fhir/Group/g1/$export",
            "http://localhost/fhir",
            null,
            null,
            null,
            List.of(),
            Map.of(),
            List.of(),
            false,
            ExportLevel.GROUP,
            patientIds);
    when(exportOperationValidator.validatePatientExportRequest(
            requestDetails, ExportLevel.GROUP, patientIds, null, null, null, null, null, null))
        .thenReturn(
            new PreAsyncValidation.PreAsyncValidationResult<>(
                exportRequest, Collections.emptyList()));

    // Args: [groupId, outputFormat, since, until, type, typeFilter, elements, requestDetails].
    final Object[] args =
        new Object[] {groupId, null, null, null, null, null, null, requestDetails};

    final PreAsyncValidation.PreAsyncValidationResult<ExportRequest> result =
        provider.preAsyncValidate(requestDetails, args);

    assertNotNull(result);
  }

  // ---------------------------------------------------------------------------
  // Read authority for the group.
  // ---------------------------------------------------------------------------

  // A caller holding the export authority and read access to Patient, but not to Group, must be
  // refused before the group is resolved. Resolving it first would reveal whether the group exists
  // (via a 404) and who its members are (via the export output).
  @Test
  void preAsyncValidateRejectsCallerWithoutGroupReadAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:export", "pathling:read:Patient");
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);

    assertThatThrownBy(() -> provider.preAsyncValidate(requestDetails, argsFor("g1")))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:read:Group");
    verifyNoInteractions(groupMemberService, exportOperationValidator);
  }

  // Read access to Group, granted directly or through a broader authority that subsumes it, lets
  // the group be resolved.
  @ParameterizedTest
  @ValueSource(strings = {"pathling:read:Group", "pathling:read", "pathling"})
  void preAsyncValidateResolvesGroupWithGroupReadAuthority(final String readAuthority) {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:export", readAuthority);
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    when(groupMemberService.extractPatientIdsFromGroup("g1")).thenReturn(Set.of("p1"));

    provider.preAsyncValidate(requestDetails, argsFor("g1"));

    verify(groupMemberService).extractPatientIdsFromGroup("g1");
  }

  // With authorisation disabled there is no token to check, and the group is resolved as before.
  @Test
  void preAsyncValidateResolvesGroupWhenAuthDisabled() {
    authConfig.setEnabled(false);
    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    when(groupMemberService.extractPatientIdsFromGroup("g1")).thenReturn(Set.of("p1"));

    provider.preAsyncValidate(requestDetails, argsFor("g1"));

    verify(groupMemberService).extractPatientIdsFromGroup("g1");
  }

  /** Builds the pre-async validation arguments for a group export with no other parameters. */
  private static Object[] argsFor(final String groupId) {
    // Args: [groupId, outputFormat, since, until, type, typeFilter, elements, requestDetails].
    return new Object[] {new IdType("Group/" + groupId), null, null, null, null, null, null, null};
  }

  private static void setSecurityContext(final String... authorities) {
    final Jwt jwt = Jwt.withTokenValue("mock").header("alg", "none").claim("sub", "user").build();
    final JwtAuthenticationToken auth =
        new JwtAuthenticationToken(jwt, AuthorityUtils.createAuthorityList(authorities));
    SecurityContextHolder.getContext().setAuthentication(auth);
  }
}

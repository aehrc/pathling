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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.PreAsyncValidation;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

  private GroupExportProvider provider;

  @BeforeEach
  void setUp() {
    provider =
        new GroupExportProvider(
            exportOperationValidator, exportOperationHelper, deltaLake, groupMemberService);
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
}

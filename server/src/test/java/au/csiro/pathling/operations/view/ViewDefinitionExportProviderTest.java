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

package au.csiro.pathling.operations.view;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.AsyncJobContext;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ExportConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportResult;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ViewDefinitionExportProvider} covering cache key computation, the export
 * operation flow, and cancellation handling.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class ViewDefinitionExportProviderTest {

  @Mock private ViewDefinitionExportExecutor executor;
  @Mock private JobRegistry jobRegistry;
  @Mock private RequestTagFactory requestTagFactory;
  @Mock private ExportResultRegistry exportResultRegistry;
  @Mock private ServerConfiguration serverConfiguration;
  @Mock private FhirContext fhirContext;
  @Mock private QueryableDataSource deltaLake;
  @Mock private GroupMemberService groupMemberService;
  @Mock private ServletRequestDetails requestDetails;

  private ViewDefinitionExportProvider provider;

  @BeforeEach
  void setUp() {
    provider =
        new ViewDefinitionExportProvider(
            executor,
            jobRegistry,
            requestTagFactory,
            exportResultRegistry,
            serverConfiguration,
            fhirContext,
            deltaLake,
            groupMemberService);
  }

  @AfterEach
  void tearDown() {
    AsyncJobContext.clear();
  }

  // -- computeCacheKeyComponent --

  @Test
  void computeCacheKeyIncludesViewsAndFormat() {
    // The cache key should include serialised views and format.
    final FhirView view = mock(FhirView.class);
    lenient().when(view.getResource()).thenReturn("Patient");
    final ViewInput viewInput = new ViewInput("my-view", view);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://localhost/fhir/$viewdefinition-export",
            "http://localhost/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.NDJSON,
            true,
            Set.of(),
            null);

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("views="));
    assertTrue(cacheKey.contains("|format=NDJSON"));
    assertTrue(cacheKey.contains("|header=true"));
  }

  @Test
  void computeCacheKeyIncludesClientTrackingId() {
    // A request with a clientTrackingId should include it in the cache key.
    final FhirView view = mock(FhirView.class);
    final ViewInput viewInput = new ViewInput(null, view);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://localhost/fhir/$viewdefinition-export",
            "http://localhost/fhir",
            List.of(viewInput),
            "tracking-123",
            ViewExportFormat.CSV,
            false,
            Set.of(),
            null);

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("|clientTrackingId=tracking-123"));
    assertTrue(cacheKey.contains("|format=CSV"));
    assertTrue(cacheKey.contains("|header=false"));
  }

  @Test
  void computeCacheKeyIncludesPatientIds() {
    // A request with patient IDs should include them sorted in the cache key.
    final FhirView view = mock(FhirView.class);
    final ViewInput viewInput = new ViewInput(null, view);
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://localhost/fhir/$viewdefinition-export",
            "http://localhost/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.NDJSON,
            true,
            Set.of("patient-b", "patient-a"),
            null);

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("|patientIds=[patient-a,patient-b]"));
  }

  @Test
  void computeCacheKeyIncludesSince() {
    // A request with a _since parameter should include it in the cache key.
    final FhirView view = mock(FhirView.class);
    final ViewInput viewInput = new ViewInput(null, view);
    final InstantType since = new InstantType("2024-01-01T00:00:00Z");
    final ViewDefinitionExportRequest request =
        new ViewDefinitionExportRequest(
            "http://localhost/fhir/$viewdefinition-export",
            "http://localhost/fhir",
            List.of(viewInput),
            null,
            ViewExportFormat.NDJSON,
            true,
            Set.of(),
            since);

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("|since="));
  }

  // -- export --

  @SuppressWarnings("unchecked")
  @Test
  void exportReturnsResultFromAsyncContext() {
    // When a job is available from the async context, export should use it.
    final Job<ViewDefinitionExportRequest> job = mock(Job.class);
    final ViewDefinitionExportRequest exportRequest =
        new ViewDefinitionExportRequest(
            "http://localhost/fhir/$viewdefinition-export",
            "http://localhost/fhir",
            List.of(),
            null,
            ViewExportFormat.NDJSON,
            true,
            Set.of(),
            null);
    final ViewDefinitionExportResponse response = mock(ViewDefinitionExportResponse.class);
    final Parameters outputParams = new Parameters();
    final ExportConfiguration exportConfig = new ExportConfiguration();
    final AuthorizationConfiguration authConfig = new AuthorizationConfiguration();

    when(job.getPreAsyncValidationResult()).thenReturn(exportRequest);
    when(job.isCancelled()).thenReturn(false);
    when(job.getId()).thenReturn("job-1");
    when(job.getOwnerId()).thenReturn(Optional.empty());
    when(executor.execute(exportRequest, "job-1")).thenReturn(List.of());
    lenient().when(serverConfiguration.getExport()).thenReturn(exportConfig);
    lenient().when(serverConfiguration.getAuth()).thenReturn(authConfig);

    AsyncJobContext.setCurrentJob(job);

    final Parameters result =
        provider.export(null, null, null, null, null, null, null, null, requestDetails);

    assertNotNull(result);
    verify(exportResultRegistry).put(eq("job-1"), any(ExportResult.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void exportReturnsNullWhenCancelled() {
    // A cancelled job should result in a null return value.
    final Job<ViewDefinitionExportRequest> job = mock(Job.class);
    final ViewDefinitionExportRequest exportRequest =
        new ViewDefinitionExportRequest(
            "http://localhost/fhir/$viewdefinition-export",
            "http://localhost/fhir",
            List.of(),
            null,
            ViewExportFormat.NDJSON,
            true,
            Set.of(),
            null);

    when(job.getPreAsyncValidationResult()).thenReturn(exportRequest);
    when(job.isCancelled()).thenReturn(true);
    lenient().when(job.getOwnerId()).thenReturn(Optional.empty());

    AsyncJobContext.setCurrentJob(job);

    final Parameters result =
        provider.export(null, null, null, null, null, null, null, null, requestDetails);

    assertNull(result);
  }

  @Test
  void exportThrowsWhenNoViewsProvided() {
    // When no views are provided in the request, preAsyncValidate should throw.
    lenient().when(requestDetails.getResource()).thenReturn(new Parameters());

    assertThrows(
        InvalidRequestException.class,
        () -> provider.export(null, null, null, null, null, null, null, null, requestDetails));
  }
}

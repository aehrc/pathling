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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.AsyncJobContext;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ExportConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.Optional;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Tests for {@link ExportOperationHelper} covering export execution, async context handling, and
 * job cancellation.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class ExportOperationHelperTest {

  @Mock private ExportExecutor exportExecutor;
  @Mock private JobRegistry jobRegistry;
  @Mock private RequestTagFactory requestTagFactory;
  @Mock private ExportResultRegistry exportResultRegistry;
  @Mock private ServerConfiguration serverConfiguration;
  @Mock private ServletRequestDetails requestDetails;

  private ExportOperationHelper helper;

  @BeforeEach
  void setUp() {
    helper =
        new ExportOperationHelper(
            exportExecutor,
            jobRegistry,
            requestTagFactory,
            exportResultRegistry,
            serverConfiguration);
  }

  @AfterEach
  void tearDown() {
    // Clear any async job context and security context set during tests.
    AsyncJobContext.clear();
    SecurityContextHolder.clearContext();
  }

  @SuppressWarnings("unchecked")
  @Test
  void executeExportUsingAsyncJobContext() {
    // When a job is available from the async context, it should be used directly.
    final Job<ExportRequest> job = mock(Job.class);
    final ExportRequest exportRequest = mock(ExportRequest.class);
    final ExportResponse exportResponse = mock(ExportResponse.class);
    final Parameters parameters = new Parameters();
    final ExportConfiguration exportConfig = new ExportConfiguration();

    when(job.getPreAsyncValidationResult()).thenReturn(exportRequest);
    when(job.isCancelled()).thenReturn(false);
    when(job.getId()).thenReturn("job-1");
    when(job.getOwnerId()).thenReturn(Optional.empty());
    when(exportExecutor.execute(exportRequest, "job-1")).thenReturn(exportResponse);
    when(exportResponse.toOutput()).thenReturn(parameters);
    lenient().when(serverConfiguration.getExport()).thenReturn(exportConfig);

    AsyncJobContext.setCurrentJob(job);

    final Parameters result = helper.executeExport(requestDetails);

    assertNotNull(result);
    verify(exportExecutor).execute(exportRequest, "job-1");
    verify(exportResultRegistry).put(eq("job-1"), any(ExportResult.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void executeExportFallsBackToJobRegistry() {
    // When no async context job is available, the helper should fall back to the job registry.
    final Job<ExportRequest> job = mock(Job.class);
    final ExportRequest exportRequest = mock(ExportRequest.class);
    final ExportResponse exportResponse = mock(ExportResponse.class);
    final Parameters parameters = new Parameters();
    final ExportConfiguration exportConfig = new ExportConfiguration();
    final RequestTag requestTag = mock(RequestTag.class);

    when(requestTagFactory.createTag(eq(requestDetails), any())).thenReturn(requestTag);
    doReturn(job).when(jobRegistry).get(requestTag);
    when(job.getPreAsyncValidationResult()).thenReturn(exportRequest);
    when(job.isCancelled()).thenReturn(false);
    when(job.getId()).thenReturn("job-2");
    when(job.getOwnerId()).thenReturn(Optional.empty());
    when(exportExecutor.execute(exportRequest, "job-2")).thenReturn(exportResponse);
    when(exportResponse.toOutput()).thenReturn(parameters);
    lenient().when(serverConfiguration.getExport()).thenReturn(exportConfig);

    final Parameters result = helper.executeExport(requestDetails);

    assertNotNull(result);
  }

  @SuppressWarnings("unchecked")
  @Test
  void executeExportReturnsNullWhenJobCancelled() {
    // A cancelled job should result in a null return value.
    final Job<ExportRequest> job = mock(Job.class);
    final ExportRequest exportRequest = mock(ExportRequest.class);

    when(job.getPreAsyncValidationResult()).thenReturn(exportRequest);
    when(job.isCancelled()).thenReturn(true);

    AsyncJobContext.setCurrentJob(job);

    final Parameters result = helper.executeExport(requestDetails);

    assertNull(result);
  }

  @Test
  void executeExportThrowsWhenNoJobFound() {
    // When neither async context nor job registry has a matching job, an exception should be
    // thrown.
    when(requestTagFactory.createTag(eq(requestDetails), any())).thenReturn(mock(RequestTag.class));
    when(jobRegistry.get(any(RequestTag.class))).thenReturn(null);

    assertThrows(InvalidRequestException.class, () -> helper.executeExport(requestDetails));
  }
}

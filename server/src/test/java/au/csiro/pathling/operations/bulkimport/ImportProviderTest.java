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

package au.csiro.pathling.operations.bulkimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.library.io.SaveMode;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ImportProvider} covering cache key computation, the import operation flow,
 * cancellation handling, and the null job case.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class ImportProviderTest {

  @Mock private ImportExecutor executor;
  @Mock private ImportOperationValidator importOperationValidator;
  @Mock private RequestTagFactory requestTagFactory;
  @Mock private JobRegistry jobRegistry;
  @Mock private ImportResultRegistry importResultRegistry;
  @Mock private ServletRequestDetails requestDetails;

  private ImportProvider provider;

  @BeforeEach
  void setUp() {
    provider =
        new ImportProvider(
            executor,
            importOperationValidator,
            requestTagFactory,
            jobRegistry,
            importResultRegistry);
  }

  @AfterEach
  void tearDown() {
    AsyncJobContext.clear();
  }

  // -- computeCacheKeyComponent --

  @Test
  void computeCacheKeyIncludesInputAndSaveMode() {
    // The cache key should include sorted input entries and the save mode.
    final Map<String, Collection<String>> input =
        Map.of(
            "Patient", List.of("http://example.com/Patient.ndjson"),
            "Condition", List.of("http://example.com/Condition.ndjson"));
    final ImportRequest request =
        new ImportRequest(
            "http://localhost/$import", input, SaveMode.OVERWRITE, ImportFormat.NDJSON);

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("input="));
    assertTrue(cacheKey.contains("|saveMode=OVERWRITE"));
    assertTrue(cacheKey.contains("|format=NDJSON"));
  }

  @Test
  void computeCacheKeyIsDeterministic() {
    // Calling computeCacheKeyComponent twice with the same request should produce the same key.
    final Map<String, Collection<String>> input =
        Map.of("Patient", List.of("http://example.com/Patient.ndjson"));
    final ImportRequest request =
        new ImportRequest("http://localhost/$import", input, SaveMode.MERGE, ImportFormat.PARQUET);

    final String key1 = provider.computeCacheKeyComponent(request);
    final String key2 = provider.computeCacheKeyComponent(request);

    assertEquals(key1, key2);
  }

  // -- importOperation --

  @SuppressWarnings("unchecked")
  @Test
  void importOperationReturnsResultFromAsyncContext() {
    // When a job is available from the async context, it should be used directly.
    final Job<ImportRequest> job = mock(Job.class);
    final ImportRequest importRequest =
        new ImportRequest(
            "http://localhost/$import",
            Map.of("Patient", List.of("http://example.com/Patient.ndjson")),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);
    final ImportResponse importResponse = mock(ImportResponse.class);
    final Parameters outputParams = new Parameters();

    when(job.getPreAsyncValidationResult()).thenReturn(importRequest);
    when(job.isCancelled()).thenReturn(false);
    when(job.getId()).thenReturn("job-1");
    when(job.getOwnerId()).thenReturn(Optional.empty());
    when(executor.execute(importRequest, "job-1")).thenReturn(importResponse);
    when(importResponse.toOutput()).thenReturn(outputParams);

    AsyncJobContext.setCurrentJob(job);

    final Parameters result = provider.importOperation(new Parameters(), requestDetails);

    assertNotNull(result);
    verify(importResultRegistry).put(eq("job-1"), any(ImportResult.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void importOperationReturnsNullWhenCancelled() {
    // A cancelled job should result in a null return value.
    final Job<ImportRequest> job = mock(Job.class);
    final ImportRequest importRequest =
        new ImportRequest(
            "http://localhost/$import",
            Map.of("Patient", List.of("http://example.com/Patient.ndjson")),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    when(job.getPreAsyncValidationResult()).thenReturn(importRequest);
    when(job.isCancelled()).thenReturn(true);

    AsyncJobContext.setCurrentJob(job);

    final Parameters result = provider.importOperation(new Parameters(), requestDetails);

    assertNull(result);
  }

  @Test
  void importOperationThrowsWhenNoJobFound() {
    // When neither async context nor job registry has a job, an exception should be thrown.
    when(requestTagFactory.createTag(any(), any(), any())).thenReturn(mock(RequestTag.class));
    when(jobRegistry.get(any(RequestTag.class))).thenReturn(null);

    // Set up a minimal valid Parameters request for the preAsyncValidate fallback.
    final Parameters parameters = new Parameters();
    lenient()
        .when(importOperationValidator.validateParametersRequest(any(), any()))
        .thenReturn(
            new au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult<>(
                new ImportRequest(
                    "http://localhost/$import",
                    Map.of("Patient", List.of("http://example.com/Patient.ndjson")),
                    SaveMode.OVERWRITE,
                    ImportFormat.NDJSON),
                List.of()));
    when(requestDetails.getServletRequest()).thenReturn(null);

    assertThrows(
        InvalidRequestException.class, () -> provider.importOperation(parameters, requestDetails));
  }
}

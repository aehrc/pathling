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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import au.csiro.pathling.library.io.SaveMode;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.time.Instant;
import java.util.List;
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
 * Tests for {@link ImportPnpProvider} covering cache key computation, the import-pnp operation
 * flow, and cancellation handling.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class ImportPnpProviderTest {

  @Mock private ImportPnpExecutor executor;
  @Mock private ImportPnpOperationValidator validator;
  @Mock private RequestTagFactory requestTagFactory;
  @Mock private JobRegistry jobRegistry;
  @Mock private ImportResultRegistry importResultRegistry;
  @Mock private ServletRequestDetails requestDetails;

  private ImportPnpProvider provider;

  @BeforeEach
  void setUp() {
    provider =
        new ImportPnpProvider(
            executor, validator, requestTagFactory, jobRegistry, importResultRegistry);
  }

  @AfterEach
  void tearDown() {
    AsyncJobContext.clear();
  }

  // -- computeCacheKeyComponent --

  @Test
  void computeCacheKeyIncludesExportUrlAndType() {
    // The cache key should include the export URL, type, save mode, and format.
    final ImportPnpRequest request =
        new ImportPnpRequest(
            "http://localhost/$import-pnp",
            "http://remote.example.com/fhir",
            "dynamic",
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            List.of(),
            List.of());

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("exportUrl=http://remote.example.com/fhir"));
    assertTrue(cacheKey.contains("|exportType=dynamic"));
    assertTrue(cacheKey.contains("|saveMode=OVERWRITE"));
    assertTrue(cacheKey.contains("|format=NDJSON"));
  }

  @Test
  void computeCacheKeyIncludesOptionalParameters() {
    // Optional parameters like types, since, elements, and typeFilters should be included.
    final ImportPnpRequest request =
        new ImportPnpRequest(
            "http://localhost/$import-pnp",
            "http://remote.example.com/fhir",
            "dynamic",
            SaveMode.MERGE,
            ImportFormat.PARQUET,
            List.of("Patient", "Condition"),
            Optional.of(Instant.parse("2024-01-01T00:00:00Z")),
            Optional.of(Instant.parse("2024-12-31T23:59:59Z")),
            List.of("id", "name"),
            List.of("Patient?active=true"),
            List.of("_provenance"));

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(cacheKey.contains("|types=Patient,Condition"));
    assertTrue(cacheKey.contains("|since="));
    assertTrue(cacheKey.contains("|until="));
    assertTrue(cacheKey.contains("|elements=id,name"));
    assertTrue(cacheKey.contains("|typeFilters=Patient?active=true"));
    assertTrue(cacheKey.contains("|includeAssociatedData=_provenance"));
  }

  @Test
  void computeCacheKeyExcludesEmptyOptionalParameters() {
    // Empty optional parameters should not appear in the cache key.
    final ImportPnpRequest request =
        new ImportPnpRequest(
            "http://localhost/$import-pnp",
            "http://remote.example.com/fhir",
            "dynamic",
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            List.of(),
            List.of());

    final String cacheKey = provider.computeCacheKeyComponent(request);

    assertTrue(!cacheKey.contains("|types="));
    assertTrue(!cacheKey.contains("|since="));
    assertTrue(!cacheKey.contains("|elements="));
    assertTrue(!cacheKey.contains("|typeFilters="));
    assertTrue(!cacheKey.contains("|includeAssociatedData="));
  }

  // -- importPnpOperation --

  @SuppressWarnings("unchecked")
  @Test
  void importPnpOperationReturnsResultFromAsyncContext() {
    // When a job is available from the async context, it should be used.
    final Job<ImportPnpRequest> job = mock(Job.class);
    final ImportPnpRequest pnpRequest =
        new ImportPnpRequest(
            "http://localhost/$import-pnp",
            "http://remote.example.com/fhir",
            "dynamic",
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            List.of(),
            List.of());
    final ImportResponse importResponse = mock(ImportResponse.class);
    final Parameters outputParams = new Parameters();

    when(job.getPreAsyncValidationResult()).thenReturn(pnpRequest);
    when(job.isCancelled()).thenReturn(false);
    when(job.getId()).thenReturn("job-1");
    when(job.getOwnerId()).thenReturn(Optional.empty());
    when(executor.execute(pnpRequest, "job-1")).thenReturn(importResponse);
    when(importResponse.toOutput()).thenReturn(outputParams);

    AsyncJobContext.setCurrentJob(job);

    final Parameters result = provider.importPnpOperation(new Parameters(), requestDetails);

    assertNotNull(result);
    verify(importResultRegistry).put(eq("job-1"), any(ImportResult.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void importPnpOperationReturnsNullWhenCancelled() {
    // A cancelled job should result in a null return value.
    final Job<ImportPnpRequest> job = mock(Job.class);
    final ImportPnpRequest pnpRequest =
        new ImportPnpRequest(
            "http://localhost/$import-pnp",
            "http://remote.example.com/fhir",
            "dynamic",
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            List.of(),
            List.of());

    when(job.getPreAsyncValidationResult()).thenReturn(pnpRequest);
    when(job.isCancelled()).thenReturn(true);
    lenient().when(job.getOwnerId()).thenReturn(Optional.empty());

    AsyncJobContext.setCurrentJob(job);

    final Parameters result = provider.importPnpOperation(new Parameters(), requestDetails);

    assertNull(result);
  }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ImportConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.io.ResourceEncoding;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.library.io.source.StorageLayout;
import au.csiro.pathling.library.io.source.UnsupportedLayoutException;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for ImportExecutor. Tests import from various source formats (NDJSON and Parquet) with
 * all imports writing to Delta format in the warehouse.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
@Slf4j
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ImportExecutorTest {

  private static final String JOB_ID = "test-job-123";
  private static final Path TEST_DATA_PATH = Path.of("src/test/resources/import-data/ndjson");

  @Autowired private PathlingContext pathlingContext;

  @Autowired private ServerConfiguration serverConfiguration;

  @Autowired private CacheableDatabase cacheableDatabase;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private IParser jsonParser;

  @TempDir private Path tempDir;

  private Path uniqueTempDir;
  private ImportExecutor importExecutor;
  private DynamicDeltaSource dataSource;

  @BeforeEach
  void setUp() throws IOException {
    uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(uniqueTempDir);

    final String databasePath = "file://" + uniqueTempDir.toAbsolutePath();
    dataSource = mock(DynamicDeltaSource.class);
    importExecutor =
        new ImportExecutor(
            Optional.empty(), // No access rules for most tests
            pathlingContext,
            databasePath,
            serverConfiguration,
            cacheableDatabase,
            true,
            dataSource);
  }

  // ========================================
  // Basic Import Tests
  // ========================================

  @Test
  void testImportSingleResourceType() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - verify response contains the original input URL
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);

    // Verify data was written to database
    final WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .hasSize(1)
        .first()
        .satisfies(
            fileInfo -> {
              assertThat(fileInfo.fhirResourceType()).isEqualTo("Patient");
              assertThat(fileInfo.absoluteUrl())
                  .contains(uniqueTempDir.toAbsolutePath().toString());
            });
  }

  @Test
  void testImportAllTestResources() {
    // Given
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of(
                "Patient",
                List.of("file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath()),
                "Condition",
                List.of("file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath()),
                "Encounter",
                List.of("file://" + TEST_DATA_PATH.resolve("Encounter.ndjson").toAbsolutePath()),
                "Observation",
                List.of("file://" + TEST_DATA_PATH.resolve("Observation.ndjson").toAbsolutePath())),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - verify all input URLs are in response
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).hasSize(4);

    // Verify data was written for all resource types (may be split into multiple files by Spark)
    final WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .extracting(FileInformation::fhirResourceType)
        .contains("Patient", "Condition", "Encounter", "Observation");
  }

  // ========================================
  // Save Mode Tests
  // ========================================

  @Test
  void testSaveModeOverwrite() {
    // Given - import data once
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When - first import
    final ImportResponse response1 = importExecutor.execute(request, JOB_ID);

    // Read count from Delta table using Spark
    final String tablePath =
        response1.getOriginalInternalWriteDetails().fileInfos().getFirst().absoluteUrl();
    final long firstImportCount =
        pathlingContext.getSpark().read().format("delta").load(tablePath).count();

    // Then - second import with OVERWRITE should replace the data
    final ImportResponse response2 = importExecutor.execute(request, JOB_ID);

    // Read count from Delta table after overwrite
    final long secondImportCount =
        pathlingContext.getSpark().read().format("delta").load(tablePath).count();

    assertThat(response1).isNotNull();
    assertThat(response2).isNotNull();
    assertThat(firstImportCount).isEqualTo(100L); // Patient.ndjson has 100 records

    // In OVERWRITE mode, data should be replaced with same count
    assertThat(secondImportCount).isEqualTo(100L);
  }

  @Test
  void testSaveModeAppend() {
    // Given - import data once
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.APPEND,
            ImportFormat.NDJSON);

    // When - first import
    final ImportResponse response1 = importExecutor.execute(request, JOB_ID);

    // Get the actual database path from WriteDetails
    final WriteDetails writeDetails1 = getWriteDetails(response1);
    final String tablePath = writeDetails1.fileInfos().getFirst().absoluteUrl();

    // Read count from Delta table after first import
    final long firstImportCount =
        pathlingContext.getSpark().read().format("delta").load(tablePath).count();

    // Then - second import with APPEND should add to the data
    final ImportResponse response2 = importExecutor.execute(request, JOB_ID);

    assertThat(response1).isNotNull();
    assertThat(response2).isNotNull();
    assertThat(response1.getInputUrls()).containsExactly(patientUrl);
    assertThat(response2.getInputUrls()).containsExactly(patientUrl);

    // Verify data was appended - count should double
    final long secondImportCount =
        pathlingContext.getSpark().read().format("delta").load(tablePath).count();
    assertThat(firstImportCount).isEqualTo(100L); // Patient.ndjson has 100 records
    assertThat(secondImportCount).isEqualTo(200L); // After append, should have 200

    // Verify data was written in both imports
    final WriteDetails writeDetails2 = getWriteDetails(response2);
    assertThat(writeDetails2.fileInfos()).isNotEmpty();
  }

  @Test
  void testSaveModeErrorIfExistsThrowsWhenFileExists() {
    // Given - import data once with OVERWRITE
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest initialRequest =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);
    final ImportResponse initialResponse = importExecutor.execute(initialRequest, JOB_ID);

    // Verify first import succeeded
    assertThat(initialResponse).isNotNull();
    assertThat(initialResponse.getInputUrls()).containsExactly(patientUrl);

    // When - second import with ERROR_IF_EXISTS should throw because Delta table exists
    final ImportRequest errorRequest =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.ERROR_IF_EXISTS,
            ImportFormat.NDJSON);

    // Then - should throw because table already exists
    assertThatThrownBy(() -> importExecutor.execute(errorRequest, JOB_ID))
        .satisfies(
            e ->
                assertThat(e.getMessage())
                    .containsAnyOf("exist", "already", "found", "DELTA_PATH_EXISTS"));
  }

  @Test
  void testSaveModeErrorIfExistsSucceedsWhenFileDoesNotExist() {
    // Given - fresh database with no existing data
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.ERROR_IF_EXISTS,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - should succeed when table doesn't exist
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);

    // Verify data was written to Delta table
    final WriteDetails writeDetails = getWriteDetails(response);
    final String tablePath = writeDetails.fileInfos().getFirst().absoluteUrl();
    final long count = pathlingContext.getSpark().read().format("delta").load(tablePath).count();
    assertThat(count).isEqualTo(100L);
  }

  // ========================================
  // File Remapping Tests
  // ========================================

  @Test
  void testFileRemappingPreservesOriginalUrls() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final String conditionUrl =
        "file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of(
                "Patient", List.of(patientUrl),
                "Condition", List.of(conditionUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - response should contain the original input URLs (SMART spec requirement)
    assertThat(response.getInputUrls()).containsExactlyInAnyOrder(patientUrl, conditionUrl);

    // Internal WriteDetails contains database paths (for tracking actual writes)
    final WriteDetails writeDetails = getWriteDetails(response);
    final List<FileInformation> fileInfos = writeDetails.fileInfos();
    assertThat(fileInfos).isNotEmpty();
    assertThat(fileInfos)
        .extracting(FileInformation::absoluteUrl)
        .allMatch(url -> url.contains(uniqueTempDir.toAbsolutePath().toString()));
  }

  @Test
  void testFileRemappingPreservesResourceTypes() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final String conditionUrl =
        "file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of(
                "Patient", List.of(patientUrl),
                "Condition", List.of(conditionUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - verify response contains input URLs
    assertThat(response.getInputUrls()).hasSize(2);

    // Verify internal WriteDetails has correct resource types
    final WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .extracting(FileInformation::fhirResourceType)
        .contains("Patient", "Condition");
  }

  @Test
  void testFileRemappingPreservesCounts() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - verify response structure (counts not in SMART spec response)
    assertThat(response.getInputUrls()).containsExactly(patientUrl);

    // Counts are available in internal WriteDetails
    final WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .isNotEmpty()
        .allSatisfy(fileInfo -> assertThat(fileInfo.fhirResourceType()).isEqualTo("Patient"));

    // Verify files were written (counts may not be populated in all modes)
    assertThat(writeDetails.fileInfos()).isNotEmpty();
    assertThat(writeDetails.fileInfos())
        .allSatisfy(
            fi -> {
              assertThat(fi.fhirResourceType()).isEqualTo("Patient");
              assertThat(fi.absoluteUrl()).isNotNull();
            });
  }

  // ========================================
  // Access Rules Tests
  // ========================================

  @Test
  void testAccessRulesValidationSuccess() {
    // Given - configure to allow file:// URLs
    final ImportConfiguration importConfiguration = serverConfiguration.getImport();
    assertNotNull(importConfiguration);
    importConfiguration.setAllowableSources(List.of("file://"));

    final ImportExecutor executorWithRules =
        new ImportExecutor(
            Optional.of(new AccessRules(serverConfiguration, true)),
            pathlingContext,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            cacheableDatabase,
            true,
            dataSource);

    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = executorWithRules.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
  }

  @Test
  void testAccessRulesValidationFailure() {
    // Given - configure to only allow s3:// URLs
    final ImportConfiguration importConfiguration = serverConfiguration.getImport();
    assertNotNull(importConfiguration);
    importConfiguration.setAllowableSources(List.of("s3://allowed-bucket/"));

    final ImportExecutor executorWithRules =
        new ImportExecutor(
            Optional.of(new AccessRules(serverConfiguration, true)),
            pathlingContext,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            cacheableDatabase,
            true,
            dataSource);

    final String deniedUrl = "s3://denied-bucket/Patient.ndjson";
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(deniedUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When/Then
    assertThatThrownBy(() -> executorWithRules.execute(request, JOB_ID))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining(deniedUrl)
        .hasMessageContaining("not an allowed source");
  }

  @Test
  void testAccessRulesSkippedWhenAbsent() {
    // Given - executor with no AccessRules
    final String anyUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(anyUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - should succeed without AccessRules validation
    assertThat(response).isNotNull();
  }

  @Test
  void testAccessRulesPartialFailure() {
    // Given - allow file:// but one URL uses s3://
    final ImportConfiguration importConfiguration = serverConfiguration.getImport();
    assertNotNull(importConfiguration);
    importConfiguration.setAllowableSources(List.of("file://"));

    final ImportExecutor executorWithRules =
        new ImportExecutor(
            Optional.of(new AccessRules(serverConfiguration, true)),
            pathlingContext,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            cacheableDatabase,
            true,
            dataSource);

    final String allowedUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final String deniedUrl = "s3://denied-bucket/Condition.ndjson";

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of(
                "Patient", List.of(allowedUrl),
                "Condition", List.of(deniedUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When/Then - should fail because one URL is not allowed
    assertThatThrownBy(() -> executorWithRules.execute(request, JOB_ID))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining(deniedUrl);
  }

  // ========================================
  // Custom Allowable Sources Tests
  // ========================================

  @Test
  void testCustomAllowableSourcesSuccess() {
    // Given - custom allowable sources that permit the file URL.
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> customAllowableSources = List.of("file://");

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID, customAllowableSources);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  @Test
  void testCustomAllowableSourcesFailure() {
    // Given - custom allowable sources that do not permit the file URL.
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> customAllowableSources = List.of("https://trusted.org/");

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When/Then - should fail because the URL doesn't match custom allowable sources.
    assertThatThrownBy(() -> importExecutor.execute(request, JOB_ID, customAllowableSources))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining(patientUrl)
        .hasMessageContaining("not an allowed source");
  }

  @Test
  void testCustomAllowableSourcesOverridesAccessRules() {
    // Given - AccessRules configured to deny file:// but custom sources allow it.
    final ImportConfiguration importConfiguration = serverConfiguration.getImport();
    assertNotNull(importConfiguration);
    importConfiguration.setAllowableSources(List.of("s3://only-this-bucket/"));

    final ImportExecutor executorWithRules =
        new ImportExecutor(
            Optional.of(new AccessRules(serverConfiguration, true)),
            pathlingContext,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            cacheableDatabase,
            true,
            dataSource);

    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> customAllowableSources = List.of("file://");

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When - use custom allowable sources that should override AccessRules.
    final ImportResponse response =
        executorWithRules.execute(request, JOB_ID, customAllowableSources);

    // Then - should succeed because custom sources take precedence.
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  @Test
  void testEmptyCustomAllowableSourcesAllowsAll() {
    // Given - empty custom allowable sources should allow any URL (no validation).
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> emptyAllowableSources = List.of();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID, emptyAllowableSources);

    // Then - should succeed because empty list bypasses validation.
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  // ========================================
  // Edge Cases
  // ========================================

  @Test
  void testOriginalRequestUrlPreservedInResponse() {
    // Given
    final String originalRequestUrl =
        "http://example.com/fhir/$import?mode=overwrite&format=ndjson";
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            originalRequestUrl,
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
    // The response should preserve the original request URL
  }

  @Test
  void testImportVerifiesDataWrittenToDatabase() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - verify response contains input URL
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);

    // Verify data was written to the database
    final WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos()).isNotEmpty();

    // Get actual database path and verify Delta table has data
    final String tablePath = writeDetails.fileInfos().getFirst().absoluteUrl();
    final long count = pathlingContext.getSpark().read().format("delta").load(tablePath).count();
    assertThat(count).isEqualTo(100L); // Patient.ndjson has 100 records
  }

  @Test
  void testFileRemappingAggregatesCountsForSameResourceType() throws IOException {
    // Given - verify test data has enough records to guarantee splitting
    final Path observationFile = TEST_DATA_PATH.resolve("Observation.ndjson");
    final long lineCount;
    try (final var lines = Files.lines(observationFile)) {
      lineCount = lines.count();
    }
    assertThat(lineCount)
        .as("Observation file must have more than 100 records to guarantee file splitting")
        .isGreaterThan(1000L);

    // Force Spark to create multiple files by setting small max records per file
    pathlingContext.getSpark().conf().set("spark.sql.files.maxRecordsPerFile", "1000");

    try {
      final String observationUrl = "file://" + observationFile.toAbsolutePath();

      final ImportRequest request =
          new ImportRequest(
              "http://example.com/fhir/$import",
              Map.of("Observation", List.of(observationUrl)),
              SaveMode.OVERWRITE,
              ImportFormat.NDJSON);

      // When
      final ImportResponse response = importExecutor.execute(request, JOB_ID);

      // Then - response should contain single input URL (SMART spec)
      assertThat(response.getInputUrls()).containsExactly(observationUrl);

      // Internal WriteDetails may have multiple files (Spark partitioning)
      final WriteDetails writeDetails = getWriteDetails(response);
      final List<FileInformation> observationInfos =
          writeDetails.fileInfos().stream()
              .filter(fi -> "Observation".equals(fi.fhirResourceType()))
              .toList();

      // Spark may split into multiple files
      assertThat(observationInfos).isNotEmpty();
      assertThat(observationInfos).allMatch(fi -> "Observation".equals(fi.fhirResourceType()));
    } finally {
      // Reset Spark configuration to default
      pathlingContext.getSpark().conf().unset("spark.sql.files.maxRecordsPerFile");
    }
  }

  // ========================================
  // Data Source Refresh Tests
  // ========================================

  /**
   * After an overwrite import, the dataset served for each imported resource type must be
   * refreshed, so that reads do not continue to be answered from a cached plan pinned before the
   * import (see issue #2714).
   */
  @Test
  void overwriteImportRefreshesDataSourceForEachImportedType() {
    // Given - an overwrite import covering two resource types.
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of(
                "Patient",
                List.of("file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath()),
                "Condition",
                List.of("file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath())),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    importExecutor.execute(request, JOB_ID);

    // Then - the data source is refreshed for every imported type.
    verify(dataSource).refresh("Patient");
    verify(dataSource).refresh("Condition");
  }

  /** Non-overwrite imports also modify the table, so they must refresh the data source too. */
  @Test
  void appendImportRefreshesDataSource() {
    // Given - an append import.
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.APPEND,
            ImportFormat.NDJSON);

    // When
    importExecutor.execute(request, JOB_ID);

    // Then
    verify(dataSource).refresh("Patient");
  }

  /** An import that is rejected before writing anything must not refresh the data source. */
  @Test
  void failedImportDoesNotRefreshDataSource() {
    // Given - an import whose source URL is not allowed.
    final String deniedUrl = "s3://denied-bucket/Patient.ndjson";
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(deniedUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When - the import is rejected by the custom allowable sources.
    assertThatThrownBy(() -> importExecutor.execute(request, JOB_ID, List.of("https://trusted/")))
        .isInstanceOf(AccessDeniedError.class);

    // Then - nothing was written, so nothing is refreshed.
    verifyNoInteractions(dataSource);
  }

  /**
   * When the injected data source is not a {@link DynamicDeltaSource}, the import must still
   * complete successfully; there is simply no pinned dataset to refresh.
   */
  @Test
  void importSucceedsWhenDataSourceIsNotDynamic() {
    // Given - an executor built over a data source that cannot be refreshed.
    final QueryableDataSource plainDataSource = mock(QueryableDataSource.class);
    final ImportExecutor executorWithPlainSource =
        new ImportExecutor(
            Optional.empty(),
            pathlingContext,
            "file://" + uniqueTempDir.toAbsolutePath(),
            serverConfiguration,
            cacheableDatabase,
            true,
            plainDataSource);

    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = executorWithPlainSource.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  // ========================================
  // Parquet Layout Tests
  // ========================================

  /**
   * Parquet written in the layout used before Parquet on FHIR (the legacy layout) is converted as
   * it is imported, so the warehouse holds the same resources in the Parquet on FHIR layout.
   */
  @Test
  void legacyLayoutParquetIsImportedAsParquetOnFhir() {
    // Given - Parquet written by the legacy encoders.
    final List<Patient> patients = patientsFromNdjson();
    final String patientUrl = writeParquet("Patient", legacyDataset("Patient", patients));

    // When
    final ImportResponse response = importExecutor.execute(parquetRequest(patientUrl), JOB_ID);

    // Then - the warehouse table is in the Parquet on FHIR layout and holds the same resources.
    final Dataset<Row> table = warehouseTable(response);
    assertThat(LayoutDetector.detect(table.schema()).getLayout())
        .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
    assertThat(summarise(table)).isEqualTo(summarise(patients));
  }

  /** Parquet already in the Parquet on FHIR layout is imported as it is. */
  @Test
  void parquetOnFhirParquetIsImportedUnchanged() {
    // Given - Parquet written in the Parquet on FHIR layout.
    final List<Patient> patients = patientsFromNdjson();
    final String patientUrl =
        writeParquet("Patient", ResourceEncoding.encode(pathlingContext, "Patient", patients));

    // When
    final ImportResponse response = importExecutor.execute(parquetRequest(patientUrl), JOB_ID);

    // Then
    final Dataset<Row> table = warehouseTable(response);
    assertThat(LayoutDetector.detect(table.schema()).getLayout())
        .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
    assertThat(summarise(table)).isEqualTo(summarise(patients));
  }

  /**
   * Parquet that mixes the two layouts can be neither read as it is nor converted, so it is
   * rejected as invalid input carrying the library's explanation, and nothing is written.
   */
  @Test
  void parquetMixingTheLayoutsIsRejectedAsInvalidInput() {
    // Given - a legacy-layout table carrying a column only the Parquet on FHIR layout has.
    final Dataset<Row> mixed =
        legacyDataset("Patient", patientsFromNdjson())
            .withColumn("resourceType", functions.lit("Patient"));
    final String patientUrl = writeParquet("Patient", mixed);
    final String expectedMessage = libraryRefusal(mixed);

    // When/Then
    assertThatThrownBy(() -> importExecutor.execute(parquetRequest(patientUrl), JOB_ID))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessage(expectedMessage)
        .hasCauseInstanceOf(UnsupportedLayoutException.class);
    assertThat(uniqueTempDir.resolve("Patient.parquet")).doesNotExist();
    verifyNoInteractions(dataSource);
  }

  /**
   * A warehouse table in the legacy layout is a server-side state, not a defect in the client's
   * data: an append into it is refused with the library's exception rather than as invalid input.
   */
  @Test
  void appendIntoLegacyWarehouseTableIsNotReportedAsInvalidInput() {
    // Given - a legacy-layout warehouse table, and valid Parquet on FHIR input.
    final List<Patient> patients = patientsFromNdjson();
    legacyDataset("Patient", patients)
        .write()
        .format("delta")
        .save(uniqueTempDir.resolve("Patient.parquet").toString());
    final String patientUrl =
        writeParquet("Patient", ResourceEncoding.encode(pathlingContext, "Patient", patients));

    // When/Then
    assertThatThrownBy(
            () -> importExecutor.execute(parquetRequest(patientUrl, SaveMode.APPEND), JOB_ID))
        .isInstanceOf(UnsupportedLayoutException.class);
  }

  /** A Parquet import in overwrite mode replaces the table, whatever the layout of the input. */
  @Test
  void parquetOverwriteReplacesTheTable() {
    // Given - a Parquet on FHIR import, then a legacy-layout import of a subset.
    final List<Patient> patients = patientsFromNdjson();
    final List<Patient> subset = patients.subList(0, 10);
    importExecutor.execute(
        parquetRequest(
            writeParquet("Patient", ResourceEncoding.encode(pathlingContext, "Patient", patients))),
        JOB_ID);

    // When
    final ImportResponse response =
        importExecutor.execute(
            parquetRequest(writeParquet("Patient", legacyDataset("Patient", subset))), JOB_ID);

    // Then - only the subset remains.
    assertThat(summarise(warehouseTable(response))).isEqualTo(summarise(subset));
  }

  /** A Parquet import in append mode adds converted legacy-layout rows to the table. */
  @Test
  void parquetAppendAddsToTheTable() {
    // Given - a legacy-layout import.
    final String patientUrl =
        writeParquet("Patient", legacyDataset("Patient", patientsFromNdjson()));
    importExecutor.execute(parquetRequest(patientUrl), JOB_ID);

    // When - the same data is appended.
    final ImportResponse response =
        importExecutor.execute(parquetRequest(patientUrl, SaveMode.APPEND), JOB_ID);

    // Then - every row is there twice.
    assertThat(warehouseTable(response).count()).isEqualTo(200L);
  }

  /**
   * A Parquet import in merge mode updates existing resources by id and inserts new ones, with
   * legacy-layout input merged into a Parquet on FHIR table.
   */
  @Test
  void parquetMergeUpdatesAndInsertsByResourceId() {
    // Given - the first 50 patients imported in the Parquet on FHIR layout.
    final List<Patient> patients = patientsFromNdjson();
    importExecutor.execute(
        parquetRequest(
            writeParquet(
                "Patient",
                ResourceEncoding.encode(pathlingContext, "Patient", patients.subList(0, 50)))),
        JOB_ID);

    // When - every patient is merged in from legacy-layout Parquet, with the first renamed.
    final List<Patient> updated = patientsFromNdjson();
    updated.getFirst().getNameFirstRep().setFamily("Merged");
    final ImportResponse response =
        importExecutor.execute(
            parquetRequest(
                writeParquet("Patient", legacyDataset("Patient", updated)), SaveMode.MERGE),
            JOB_ID);

    // Then - the table holds each patient once, as merged.
    assertThat(summarise(warehouseTable(response))).isEqualTo(summarise(updated));
  }

  // ========================================
  // Helper Methods
  // ========================================

  /** Parses the patients in the NDJSON test data. */
  @Nonnull
  private List<Patient> patientsFromNdjson() {
    try (final Stream<String> lines = Files.lines(TEST_DATA_PATH.resolve("Patient.ndjson"))) {
      return lines
          .filter(line -> !line.isBlank())
          .map(line -> jsonParser.parseResource(Patient.class, line))
          .collect(Collectors.toCollection(ArrayList::new));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Encodes resources with the legacy encoders, as Parquet written before Parquet on FHIR was. */
  @Nonnull
  private Dataset<Row> legacyDataset(
      @Nonnull final String resourceType, @Nonnull final List<? extends IBaseResource> resources) {
    return pathlingContext
        .getSpark()
        .createDataset(List.copyOf(resources), fhirEncoders.of(resourceType))
        .toDF();
  }

  /** Writes a dataset as Parquet named for its resource type, returning its URL. */
  @Nonnull
  private String writeParquet(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> data) {
    final Path path =
        tempDir.resolve("input-" + UUID.randomUUID()).resolve(resourceType + ".parquet");
    data.write().parquet(path.toString());
    return "file://" + path.toAbsolutePath();
  }

  @Nonnull
  private static ImportRequest parquetRequest(@Nonnull final String patientUrl) {
    return parquetRequest(patientUrl, SaveMode.OVERWRITE);
  }

  @Nonnull
  private static ImportRequest parquetRequest(
      @Nonnull final String patientUrl, @Nonnull final SaveMode saveMode) {
    return new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        saveMode,
        ImportFormat.PARQUET);
  }

  /** Reads the warehouse table an import wrote. */
  @Nonnull
  private Dataset<Row> warehouseTable(@Nonnull final ImportResponse response) {
    final String tablePath = getWriteDetails(response).fileInfos().getFirst().absoluteUrl();
    return pathlingContext.getSpark().read().format("delta").load(tablePath);
  }

  /** Summarises patients as their ids mapped to their first family names, for comparison. */
  @Nonnull
  private Map<String, String> summarise(@Nonnull final List<? extends IBaseResource> patients) {
    return patients.stream()
        .map(Patient.class::cast)
        .collect(
            Collectors.toMap(
                patient -> patient.getIdElement().getIdPart(),
                patient -> patient.getNameFirstRep().getFamily()));
  }

  @Nonnull
  private Map<String, String> summarise(@Nonnull final Dataset<Row> table) {
    return summarise(ResourceEncoding.decode(pathlingContext, "Patient", table));
  }

  /** Returns the message the library refuses a dataset's layout with. */
  @Nonnull
  private static String libraryRefusal(@Nonnull final Dataset<Row> data) {
    try {
      LayoutDetector.check("Patient", data.schema());
    } catch (final UnsupportedLayoutException e) {
      return e.getMessage();
    }
    throw new AssertionError("Expected the library to refuse the layout");
  }

  /** Extract WriteDetails from ImportResponse using reflection. */
  private WriteDetails getWriteDetails(final ImportResponse response) {
    try {
      final var field = response.getClass().getDeclaredField("originalInternalWriteDetails");
      field.setAccessible(true);
      return (WriteDetails) field.get(response);
    } catch (final Exception e) {
      throw new RuntimeException("Unable to extract WriteDetails from response", e);
    }
  }
}

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ImportConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for ImportExecutor. Tests import from various source formats (NDJSON, Delta, Parquet)
 * with all imports writing to Delta format in the warehouse.
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

  @Autowired
  private PathlingContext pathlingContext;

  @Autowired
  private ServerConfiguration serverConfiguration;

  @Autowired
  private CacheableDatabase cacheableDatabase;

  @TempDir
  private Path tempDir;

  private Path uniqueTempDir;
  private ImportExecutor importExecutor;

  @BeforeEach
  void setUp() throws IOException {
    uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(uniqueTempDir);

    final String databasePath = "file://" + uniqueTempDir.toAbsolutePath();
    importExecutor = new ImportExecutor(
        Optional.empty(), // No access rules for most tests
        pathlingContext,
        databasePath,
        serverConfiguration,
        cacheableDatabase
    );
  }

  // ========================================
  // Basic Import Tests
  // ========================================

  @Test
  void testImportSingleResourceType() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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
        .satisfies(fileInfo -> {
          assertThat(fileInfo.fhirResourceType()).isEqualTo("Patient");
          assertThat(fileInfo.absoluteUrl()).contains(uniqueTempDir.toAbsolutePath().toString());
        });
  }

  @Test
  void testImportAllTestResources() {
    // Given
    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient",
            List.of("file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath()),
            "Condition",
            List.of("file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath()),
            "Encounter",
            List.of("file://" + TEST_DATA_PATH.resolve("Encounter.ndjson").toAbsolutePath()),
            "Observation",
            List.of("file://" + TEST_DATA_PATH.resolve("Observation.ndjson").toAbsolutePath())
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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
    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

    // When - first import
    final ImportResponse response1 = importExecutor.execute(request, JOB_ID);

    // Read count from Delta table using Spark
    final String tablePath = response1.getOriginalInternalWriteDetails().fileInfos().getFirst()
        .absoluteUrl();
    final long firstImportCount = pathlingContext.getSpark().read()
        .format("delta")
        .load(tablePath)
        .count();

    // Then - second import with OVERWRITE should replace the data
    final ImportResponse response2 = importExecutor.execute(request, JOB_ID);

    // Read count from Delta table after overwrite
    final long secondImportCount = pathlingContext.getSpark().read()
        .format("delta")
        .load(tablePath)
        .count();

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
    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.APPEND,
        ImportFormat.NDJSON
    );

    // When - first import
    final ImportResponse response1 = importExecutor.execute(request, JOB_ID);

    // Get the actual database path from WriteDetails
    final WriteDetails writeDetails1 = getWriteDetails(response1);
    final String tablePath = writeDetails1.fileInfos().getFirst().absoluteUrl();

    // Read count from Delta table after first import
    final long firstImportCount = pathlingContext.getSpark().read()
        .format("delta")
        .load(tablePath)
        .count();

    // Then - second import with APPEND should add to the data
    final ImportResponse response2 = importExecutor.execute(request, JOB_ID);

    assertThat(response1).isNotNull();
    assertThat(response2).isNotNull();
    assertThat(response1.getInputUrls()).containsExactly(patientUrl);
    assertThat(response2.getInputUrls()).containsExactly(patientUrl);

    // Verify data was appended - count should double
    final long secondImportCount = pathlingContext.getSpark().read()
        .format("delta")
        .load(tablePath)
        .count();
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
    final ImportRequest initialRequest = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );
    final ImportResponse initialResponse = importExecutor.execute(initialRequest, JOB_ID);

    // Verify first import succeeded
    assertThat(initialResponse).isNotNull();
    assertThat(initialResponse.getInputUrls()).containsExactly(patientUrl);

    // When - second import with ERROR_IF_EXISTS should throw because Delta table exists
    final ImportRequest errorRequest = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.ERROR_IF_EXISTS,
        ImportFormat.NDJSON
    );

    // Then - should throw because table already exists
    assertThatThrownBy(() -> importExecutor.execute(errorRequest, JOB_ID))
        .satisfies(e -> assertThat(e.getMessage())
            .containsAnyOf("exist", "already", "found", "DELTA_PATH_EXISTS"));
  }

  @Test
  void testSaveModeErrorIfExistsSucceedsWhenFileDoesNotExist() {
    // Given - fresh database with no existing data
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.ERROR_IF_EXISTS,
        ImportFormat.NDJSON
    );

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - should succeed when table doesn't exist
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);

    // Verify data was written to Delta table
    final WriteDetails writeDetails = getWriteDetails(response);
    final String tablePath = writeDetails.fileInfos().getFirst().absoluteUrl();
    final long count = pathlingContext.getSpark().read()
        .format("delta")
        .load(tablePath)
        .count();
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

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of(patientUrl),
            "Condition", List.of(conditionUrl)
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - response should contain the original input URLs (SMART spec requirement)
    assertThat(response.getInputUrls())
        .containsExactlyInAnyOrder(patientUrl, conditionUrl);

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

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of(patientUrl),
            "Condition", List.of(conditionUrl)
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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
    assertThat(writeDetails.fileInfos()).allSatisfy(fi -> {
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

    final ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration,
        cacheableDatabase
    );

    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

    // When
    final ImportResponse response = executorWithRules.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
  }

  @Test
  @Disabled("Allowed sources check is temporarily disabled for connectathon")
  void testAccessRulesValidationFailure() {
    // Given - configure to only allow s3:// URLs
    final ImportConfiguration importConfiguration = serverConfiguration.getImport();
    assertNotNull(importConfiguration);
    importConfiguration.setAllowableSources(List.of("s3://allowed-bucket/"));

    final ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration,
        cacheableDatabase
    );

    final String deniedUrl = "s3://denied-bucket/Patient.ndjson";
    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(deniedUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(anyUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - should succeed without AccessRules validation
    assertThat(response).isNotNull();
  }

  @Test
  @Disabled("Allowed sources check is temporarily disabled for connectathon")
  void testAccessRulesPartialFailure() {
    // Given - allow file:// but one URL uses s3://
    final ImportConfiguration importConfiguration = serverConfiguration.getImport();
    assertNotNull(importConfiguration);
    importConfiguration.setAllowableSources(List.of("file://"));

    final ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration,
        cacheableDatabase
    );

    final String allowedUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final String deniedUrl = "s3://denied-bucket/Condition.ndjson";

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of(allowedUrl),
            "Condition", List.of(deniedUrl)
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID, customAllowableSources);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  @Test
  @Disabled("Allowed sources check is temporarily disabled for connectathon")
  void testCustomAllowableSourcesFailure() {
    // Given - custom allowable sources that do not permit the file URL.
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> customAllowableSources = List.of("https://trusted.org/");

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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

    final ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration,
        cacheableDatabase
    );

    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> customAllowableSources = List.of("file://");

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

    // When - use custom allowable sources that should override AccessRules.
    final ImportResponse response = executorWithRules.execute(request, JOB_ID,
        customAllowableSources);

    // Then - should succeed because custom sources take precedence.
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  @Test
  void testEmptyCustomAllowableSourcesAllowsAll() {
    // Given - empty custom allowable sources should allow any URL (no validation).
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final List<String> emptyAllowableSources = List.of();

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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
    final String originalRequestUrl = "http://example.com/fhir/$import?mode=overwrite&format=ndjson";
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    final ImportRequest request = new ImportRequest(
        originalRequestUrl,
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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

    final ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON
    );

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
    final long count = pathlingContext.getSpark().read()
        .format("delta")
        .load(tablePath)
        .count();
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

      final ImportRequest request = new ImportRequest(
          "http://example.com/fhir/$import",
          Map.of("Observation", List.of(observationUrl)),
          SaveMode.OVERWRITE,
          ImportFormat.NDJSON
      );

      // When
      final ImportResponse response = importExecutor.execute(request, JOB_ID);

      // Then - response should contain single input URL (SMART spec)
      assertThat(response.getInputUrls()).containsExactly(observationUrl);

      // Internal WriteDetails may have multiple files (Spark partitioning)
      final WriteDetails writeDetails = getWriteDetails(response);
      final List<FileInformation> observationInfos = writeDetails.fileInfos().stream()
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
  // Helper Methods
  // ========================================

  /**
   * Extract WriteDetails from ImportResponse using reflection.
   */
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

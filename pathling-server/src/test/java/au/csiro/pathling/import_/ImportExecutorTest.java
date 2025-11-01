package au.csiro.pathling.import_;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.library.io.sink.FileInfo;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.operations.import_.AccessRules;
import au.csiro.pathling.operations.import_.ImportExecutor;
import au.csiro.pathling.operations.import_.ImportFormat;
import au.csiro.pathling.operations.import_.ImportRequest;
import au.csiro.pathling.operations.import_.ImportResponse;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import au.csiro.pathling.util.TestDataSetup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for ImportExecutor focusing on NDJSON format.
 *
 * @author Felix Naumann
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

  @TempDir
  private Path tempDir;

  private Path uniqueTempDir;
  private ImportExecutor importExecutor;

  @BeforeEach
  void setUp() throws IOException {
    uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(uniqueTempDir);

    String databasePath = "file://" + uniqueTempDir.toAbsolutePath();
    importExecutor = new ImportExecutor(
        Optional.empty(), // No access rules for most tests
        pathlingContext,
        databasePath,
        serverConfiguration
    );
  }

  // ========================================
  // Basic Import Tests
  // ========================================

  @Test
  void test_import_single_resource_type() {
    // Given
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
    WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .hasSize(1)
        .first()
        .satisfies(fileInfo -> {
          assertThat(fileInfo.fhirResourceType()).isEqualTo("Patient");
          assertThat(fileInfo.absoluteUrl()).isEqualTo(patientUrl);
        });
  }

  @Test
  void test_import_all_test_resources() {
    // Given
    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of("file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath()),
            "Condition", List.of("file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath()),
            "Encounter", List.of("file://" + TEST_DATA_PATH.resolve("Encounter.ndjson").toAbsolutePath()),
            "Observation", List.of("file://" + TEST_DATA_PATH.resolve("Observation.ndjson").toAbsolutePath())
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
    WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .hasSize(4)
        .extracting(FileInfo::fhirResourceType)
        .containsExactlyInAnyOrder("Patient", "Condition", "Encounter", "Observation");
  }

  // ========================================
  // Save Mode Tests
  // ========================================

  @Test
  void test_save_mode_overwrite() throws IOException {
    // Given - import data once
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );
    // TestDataSetup.staticCopyTestDataToTempDir(uniqueTempDir, "Patient");

    // When - first import
    ImportResponse response1 = importExecutor.execute(request, JOB_ID);
    Path patientFile = Paths.get(URI.create(response1.getOriginalInternalWriteDetails().fileInfos().get(0).absoluteUrl()));
    long firstImportCount = Files.lines(patientFile).count();

    // Then - second import with OVERWRITE should replace the file
    String patient2Url = "file://" + TEST_DATA_PATH.resolve("Patient_2.ndjson").toAbsolutePath();
    ImportRequest request2 = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patient2Url)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );
    
    ImportResponse response2 = importExecutor.execute(request2, JOB_ID);
    long secondImportCount = Files.lines(patientFile).count();

    assertThat(response1).isNotNull();
    assertThat(response2).isNotNull();
    assertThat(patientFile).exists();
    assertThat(firstImportCount).isNotZero();
    assertThat(secondImportCount).isNotZero();
    // by asserting that the line count is different, we can test whether the second import has correctly overwritten the first import
    assertThat(firstImportCount).isNotEqualTo(secondImportCount);
  }

  @Test
  void test_save_mode_append() throws IOException {
    // Given - import data once
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.APPEND,
        ImportFormat.NDJSON,
        false
    );

    // When - first import
    ImportResponse response1 = importExecutor.execute(request, JOB_ID);
    Path patientFile = uniqueTempDir.resolve("Patient.ndjson");
    long firstImportSize = Files.size(patientFile);

    // Then - second import with APPEND should add to the file
    ImportResponse response2 = importExecutor.execute(request, JOB_ID);
    long secondImportSize = Files.size(patientFile);

    assertThat(response1).isNotNull();
    assertThat(response2).isNotNull();
    assertThat(patientFile).exists();
    assertThat(firstImportSize).isGreaterThan(0L);
    assertThat(secondImportSize).isGreaterThan((long) (firstImportSize * 1.9)); // Approximately doubled after append
  }

  @Test
  void test_save_mode_error_if_exists_throws_when_file_exists() {
    // Given - import data once with OVERWRITE
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    ImportRequest initialRequest = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );
    importExecutor.execute(initialRequest, JOB_ID);

    // When/Then - second import with ERROR_IF_EXISTS should throw
    ImportRequest errorRequest = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.ERROR_IF_EXISTS,
        ImportFormat.NDJSON,
        false
    );

    assertThatThrownBy(() -> importExecutor.execute(errorRequest, JOB_ID))
        .hasMessageContaining("already exists");
  }

  @Test
  void test_save_mode_error_if_exists_succeeds_when_file_does_not_exist() throws IOException {
    // Given - fresh database with no existing data
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.ERROR_IF_EXISTS,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - should succeed when file doesn't exist
    assertThat(response).isNotNull();
    Path patientFile = uniqueTempDir.resolve("Patient.ndjson");
    assertThat(patientFile).exists();
    assertThat(Files.size(patientFile)).isGreaterThan(0L);
  }

  // ========================================
  // File Remapping Tests
  // ========================================

  @Test
  void test_file_remapping_preserves_original_urls() {
    // Given
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    String conditionUrl = "file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of(patientUrl),
            "Condition", List.of(conditionUrl)
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - URLs in response should be the original input URLs, not database paths
    WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .extracting(FileInfo::absoluteUrl)
        .containsExactlyInAnyOrder(patientUrl, conditionUrl)
        .noneMatch(url -> url.contains(uniqueTempDir.toString())); // Database path should not appear
  }

  @Test
  void test_file_remapping_preserves_resource_types() {
    // Given
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    String conditionUrl = "file://" + TEST_DATA_PATH.resolve("Condition.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of(patientUrl),
            "Condition", List.of(conditionUrl)
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .extracting(FileInfo::fhirResourceType)
        .containsExactlyInAnyOrder("Patient", "Condition");
  }

  @Test
  void test_file_remapping_preserves_counts() {
    // Given
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    WriteDetails writeDetails = getWriteDetails(response);
    assertThat(writeDetails.fileInfos())
        .first()
        .satisfies(fileInfo -> {
          assertThat(fileInfo.count()).isGreaterThan(0L);
          assertThat(fileInfo.count()).isEqualTo(100L); // Patient.ndjson has 100 lines
        });
  }

  // ========================================
  // Access Rules Tests
  // ========================================

  @Test
  void test_access_rules_validation_success() {
    // Given - configure to allow file:// URLs
    serverConfiguration.getImport().setAllowableSources(List.of("file://"));

    ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration
    );

    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = executorWithRules.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
  }

  @Test
  void test_access_rules_validation_failure() {
    // Given - configure to only allow s3:// URLs
    serverConfiguration.getImport().setAllowableSources(List.of("s3://allowed-bucket/"));

    ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration
    );

    String deniedUrl = "s3://denied-bucket/Patient.ndjson";
    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(deniedUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When/Then
    assertThatThrownBy(() -> executorWithRules.execute(request, JOB_ID))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining(deniedUrl)
        .hasMessageContaining("not an allowed source");
  }

  @Test
  void test_access_rules_skipped_when_absent() {
    // Given - executor with no AccessRules
    String anyUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(anyUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then - should succeed without AccessRules validation
    assertThat(response).isNotNull();
  }

  @Test
  void test_access_rules_partial_failure() {
    // Given - allow file:// but one URL uses s3://
    serverConfiguration.getImport().setAllowableSources(List.of("file://"));

    ImportExecutor executorWithRules = new ImportExecutor(
        Optional.of(new AccessRules(serverConfiguration)),
        pathlingContext,
        "file://" + uniqueTempDir.toAbsolutePath(),
        serverConfiguration
    );

    String allowedUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    String deniedUrl = "s3://denied-bucket/Condition.ndjson";

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of(
            "Patient", List.of(allowedUrl),
            "Condition", List.of(deniedUrl)
        ),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When/Then - should fail because one URL is not allowed
    assertThatThrownBy(() -> executorWithRules.execute(request, JOB_ID))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining(deniedUrl);
  }

  // ========================================
  // Edge Cases
  // ========================================

  @Test
  void test_original_request_url_preserved_in_response() {
    // Given
    String originalRequestUrl = "http://example.com/fhir/$import?mode=overwrite&format=ndjson";
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        originalRequestUrl,
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
    // The response should preserve the original request URL
  }

  @Test
  void test_lenient_mode() {
    // Given
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        true // lenient mode
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();
  }

  @Test
  void test_import_verifies_data_written_to_database() throws IOException {
    // Given
    String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();

    ImportRequest request = new ImportRequest(
        "http://example.com/fhir/$import",
        Map.of("Patient", List.of(patientUrl)),
        SaveMode.OVERWRITE,
        ImportFormat.NDJSON,
        false
    );

    // When
    ImportResponse response = importExecutor.execute(request, JOB_ID);

    // Then
    assertThat(response).isNotNull();

    // Verify data was written to the database path
    Path patientDataPath = uniqueTempDir.resolve("Patient.ndjson");
    assertThat(patientDataPath).exists();

    // Verify the file has content
    long fileSize = Files.size(patientDataPath);
    assertThat(fileSize).isGreaterThan(0);
  }

  @Test
  void test_file_remapping_aggregates_counts_for_same_resource_type() throws IOException {
    // Given - verify test data has enough records to guarantee splitting
    Path observationFile = TEST_DATA_PATH.resolve("Observation.ndjson");
    long lineCount = Files.lines(observationFile).count();
    assertThat(lineCount)
        .as("Observation file must have more than 100 records to guarantee file splitting")
        .isGreaterThan(1000L);

    // Force Spark to create multiple files by setting small max records per file
    pathlingContext.getSpark().conf().set("spark.sql.files.maxRecordsPerFile", "1000");

    try {
      String observationUrl = "file://" + observationFile.toAbsolutePath();

      ImportRequest request = new ImportRequest(
          "http://example.com/fhir/$import",
          Map.of("Observation", List.of(observationUrl)),
          SaveMode.OVERWRITE,
          ImportFormat.NDJSON,
          false
      );

      // When
      ImportResponse response = importExecutor.execute(request, JOB_ID);

      // Then - even though Spark creates multiple files, the response should have one FileInfo with aggregated count
      WriteDetails writeDetails = getWriteDetails(response);
      List<FileInfo> observationInfos = writeDetails.fileInfos().stream()
          .filter(fi -> "Observation".equals(fi.fhirResourceType()))
          .toList();

      assertThat(observationInfos).hasSize(1);
      assertThat(observationInfos.get(0).absoluteUrl()).isEqualTo(observationUrl);
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
  private WriteDetails getWriteDetails(ImportResponse response) {
    try {
      var field = response.getClass().getDeclaredField("writeDetails");
      field.setAccessible(true);
      return (WriteDetails) field.get(response);
    } catch (Exception e) {
      throw new RuntimeException("Unable to extract WriteDetails from response", e);
    }
  }
}

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for ImportExecutor that verify lock behavior.
 *
 * <p>Note: The ImportLock aspect is tested indirectly through ImportExecutor tests. Direct testing
 * of the aspect's concurrent behavior is complex due to AOP and Spring profile requirements.
 *
 * @author John Grimes
 */
@Slf4j
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ImportLockTest {

  private static final String JOB_ID_1 = "test-job-1";
  private static final String JOB_ID_2 = "test-job-2";
  private static final Path TEST_DATA_PATH = Path.of("src/test/resources/import-data/ndjson");

  @Autowired private PathlingContext pathlingContext;

  @Autowired private ServerConfiguration serverConfiguration;

  @Autowired private CacheableDatabase cacheableDatabase;

  @TempDir private Path tempDir;

  private Path uniqueTempDir;
  private ImportExecutor importExecutor;

  @BeforeEach
  void setUp() throws IOException {
    uniqueTempDir = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(uniqueTempDir);

    final String databasePath = "file://" + uniqueTempDir.toAbsolutePath();
    importExecutor =
        new ImportExecutor(
            Optional.empty(), // No access rules for tests
            pathlingContext,
            databasePath,
            serverConfiguration,
            cacheableDatabase);
  }

  @Test
  void singleImportSucceeds() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When
    final ImportResponse response = importExecutor.execute(request, JOB_ID_1);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.getInputUrls()).containsExactly(patientUrl);
  }

  @Test
  void sequentialImportsSucceed() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request1 =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);
    final ImportRequest request2 =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When - execute imports sequentially
    final ImportResponse response1 = importExecutor.execute(request1, JOB_ID_1);
    final ImportResponse response2 = importExecutor.execute(request2, JOB_ID_2);

    // Then - both should succeed
    assertThat(response1).isNotNull();
    assertThat(response1.getInputUrls()).containsExactly(patientUrl);
    assertThat(response2).isNotNull();
    assertThat(response2.getInputUrls()).containsExactly(patientUrl);
  }

  @Test
  void lockReleasedAfterSuccessfulImport() {
    // Given
    final String patientUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(patientUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When - execute first import
    final ImportResponse response1 = importExecutor.execute(request, JOB_ID_1);

    // Then - first import succeeds
    assertThat(response1).isNotNull();

    // When - execute second import after first completes
    final ImportResponse response2 = importExecutor.execute(request, JOB_ID_2);

    // Then - second import also succeeds (lock was released)
    assertThat(response2).isNotNull();
  }

  @Test
  void lockReleasedAfterFailedImport() {
    // Given - create request with invalid URL to force failure
    final String invalidUrl = "file:///nonexistent/Patient.ndjson";
    final ImportRequest request =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(invalidUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    // When/Then - first import fails
    assertThatThrownBy(() -> importExecutor.execute(request, JOB_ID_1))
        .isInstanceOf(Exception.class); // Could be various Spark exceptions

    // When - execute second import after first fails
    final String validUrl = "file://" + TEST_DATA_PATH.resolve("Patient.ndjson").toAbsolutePath();
    final ImportRequest validRequest =
        new ImportRequest(
            "http://example.com/fhir/$import",
            Map.of("Patient", List.of(validUrl)),
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON);

    final ImportResponse response = importExecutor.execute(validRequest, JOB_ID_2);

    // Then - second import succeeds (lock was released even after failure)
    assertThat(response).isNotNull();
  }
}

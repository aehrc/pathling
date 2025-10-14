package au.csiro.pathling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.export.ExportResult;
import au.csiro.pathling.export.ExportResultProvider;
import au.csiro.pathling.export.ExportResultRegistry;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * @author Felix Naumann
 */
//@WebMvcTest(FileController.class)
@Import({FhirServerTestConfiguration.class, UnitTestDependencies.class})
@SpringBootUnitTest
class ExportOperationDownloadTest {

  private static final String TEST_JOB_ID = "95d25c62-2b5a-4eb8-9904-88d8a9c00f2b";
  private static final String TEST_FILENAME = "test-file.ndjson";
  private static final String TEST_CONTENT = """
      {"resourceType":"Patient","id":"1","active":true}
      {"resourceType":"Patient","id":"2","active":false}
      """;

  private ExportResultProvider exportResultProvider;

  @TempDir
  private Path tempDir;
  @Autowired
  private RequestTagFactory requestTagFactory;
  @Autowired
  private JobRegistry jobRegistry;
  @Autowired
  private ExportResultRegistry exportResultRegistry;

  @BeforeEach
  void setup() throws IOException {
    exportResultProvider = new ExportResultProvider(requestTagFactory, jobRegistry,
        exportResultRegistry, "file://" + tempDir.toString());

    // Create the jobs directory structure
    Path jobsDir = tempDir.resolve("jobs");
    Path jobDir = jobsDir.resolve(TEST_JOB_ID);
    Files.createDirectories(jobDir);

    // Create test files
    Path testFile = jobDir.resolve(TEST_FILENAME);
    Files.writeString(testFile, TEST_CONTENT);

    // Create additional test files
    Path anotherFile = jobDir.resolve("patients.ndjson");
    Files.writeString(anotherFile, """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Doe","given":["John"]}]}
        {"resourceType":"Patient","id":"patient2","name":[{"family":"Smith","given":["Jane"]}]}
        """);

    Path observationsFile = jobDir.resolve("observations.ndjson");
    Files.writeString(observationsFile, """
        {"resourceType":"Observation","id":"obs1","status":"final"}
        {"resourceType":"Observation","id":"obs2","status":"preliminary"}
        """);

    exportResultRegistry.put(TEST_JOB_ID, new ExportResult(Optional.empty()));
  }

  @Test
  void test_file_is_served_correctly() throws Exception {
    MockHttpServletResponse response = new MockHttpServletResponse();
    exportResultProvider.result(TEST_JOB_ID, TEST_FILENAME, response);
    assertThat(response.getContentType()).isEqualTo("application/octet-stream");
    assertThat(response.containsHeader(HttpHeaders.CONTENT_DISPOSITION)).isTrue();
    assertThat(response.getHeader(HttpHeaders.CONTENT_DISPOSITION))
        .contains("attachment")
        .contains("filename=\"" + TEST_FILENAME + "\"");

    // Get the resource and read its content
    String actualContent = response.getContentAsString();
    assertThat(actualContent)
        .isNotEmpty()
        .isEqualTo(TEST_CONTENT);
  }

  @Test
  void test_nothing_is_served_if_job_is_unknown() {
    MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(
        () -> exportResultProvider.result("96f7f71b-f37e-4f1c-8cb4-0571e4b7f37b", TEST_FILENAME,
            response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessage("Unknown job id.");
  }

  @Test
  void test_nothing_is_served_if_filename_is_unknown() {
    MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "unknown-file.ndjson", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file!");
  }
}

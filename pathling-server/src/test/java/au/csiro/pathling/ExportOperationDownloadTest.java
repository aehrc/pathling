package au.csiro.pathling;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;

/**
 * @author Felix Naumann
 */
//@WebMvcTest(FileController.class)
@Import({FhirServerTestConfiguration.class, UnitTestDependencies.class})
@SpringBootUnitTest
class ExportOperationDownloadTest {

  private static final String TEST_JOB_ID = "test-job-123";
  private static final String TEST_FILENAME = "test-file.ndjson";
  private static final String TEST_CONTENT = """
            {"resourceType":"Patient","id":"1","active":true}
            {"resourceType":"Patient","id":"2","active":false}
            """;
  
  private FileController fileController;
  
  @TempDir
  private Path tempDir;
  
  @BeforeEach
  void setup() throws IOException {
    fileController = new FileController("file://" + tempDir.toString());

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

    // Create a different job directory
    Path anotherJobDir = jobsDir.resolve("another-job-456");
    Files.createDirectories(anotherJobDir);
    Path anotherJobFile = anotherJobDir.resolve("data.ndjson");
    Files.writeString(anotherJobFile, """
                {"resourceType":"Condition","id":"cond1","clinicalStatus":{"coding":[{"code":"active"}]}}
                """);
  }

  @Test
  void test_file_is_served_correctly() throws Exception {
    ResponseEntity<Resource> response = fileController.serveFile(TEST_JOB_ID, TEST_FILENAME);
    assertThat(response)
        .isNotNull();
    assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

    // Get the resource and read its content
    Resource resource = response.getBody();
    assertThat(resource).isNotNull();
    assertThat(resource.exists()).isTrue();

    // Read file content to string
    String fileContent = Files.readString(resource.getFile().toPath());
    assertThat(fileContent).isEqualTo(TEST_CONTENT);
  }
  
  @Test
  void test_nothing_is_served_if_job_is_unknown() {
    ResponseEntity<Resource> response = fileController.serveFile("unknown-job-id-1", TEST_FILENAME);
    assertThat(response)
        .isNotNull();
    assertThat(response.getStatusCode().is4xxClientError()).isTrue();
  }
  
  @Test
  void test_nothing_is_served_if_filename_is_unknown() {
    ResponseEntity<Resource> response = fileController.serveFile(TEST_JOB_ID, "unknown-file.ndjson");
    assertThat(response)
        .isNotNull();
    assertThat(response.getStatusCode().is4xxClientError()).isTrue();
  }
}

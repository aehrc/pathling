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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import au.csiro.pathling.UnitTestDependencies;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
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
@Import({FhirServerTestConfiguration.class, UnitTestDependencies.class})
@SpringBootUnitTest
class ExportOperationDownloadTest {

  private static final String TEST_JOB_ID = "95d25c62-2b5a-4eb8-9904-88d8a9c00f2b";
  private static final String TEST_FILENAME = "test-file.ndjson";
  private static final String TEST_CONTENT =
      """
      {"resourceType":"Patient","id":"1","active":true}
      {"resourceType":"Patient","id":"2","active":false}
      """;

  private ExportResultProvider exportResultProvider;

  @TempDir private Path tempDir;

  @SuppressWarnings("unused")
  @Autowired
  private RequestTagFactory requestTagFactory;

  @SuppressWarnings("unused")
  @Autowired
  private JobRegistry jobRegistry;

  @Autowired private ExportResultRegistry exportResultRegistry;

  @Autowired private ServerConfiguration serverConfiguration;

  @BeforeEach
  void setup() throws IOException {
    final au.csiro.pathling.io.JobDirectoryFileSystem jobDirectoryFileSystem =
        new au.csiro.pathling.io.JobDirectoryFileSystem(
            tempDir.toUri(), new org.apache.hadoop.conf.Configuration());
    exportResultProvider =
        new ExportResultProvider(exportResultRegistry, jobDirectoryFileSystem, serverConfiguration);

    // Create the jobs directory structure
    final Path jobsDir = tempDir.resolve("jobs");
    final Path jobDir = jobsDir.resolve(TEST_JOB_ID);
    Files.createDirectories(jobDir);

    // Create test files
    final Path testFile = jobDir.resolve(TEST_FILENAME);
    Files.writeString(testFile, TEST_CONTENT);

    // Create additional test files
    final Path anotherFile = jobDir.resolve("patients.ndjson");
    Files.writeString(
        anotherFile,
        """
        {"resourceType":"Patient","id":"patient1","name":[{"family":"Doe","given":["John"]}]}
        {"resourceType":"Patient","id":"patient2","name":[{"family":"Smith","given":["Jane"]}]}
        """);

    final Path observationsFile = jobDir.resolve("observations.ndjson");
    Files.writeString(
        observationsFile,
        """
        {"resourceType":"Observation","id":"obs1","status":"final"}
        {"resourceType":"Observation","id":"obs2","status":"preliminary"}
        """);

    exportResultRegistry.put(TEST_JOB_ID, new ExportResult(Optional.empty()));

    // Create a marker file outside the job directory to detect traversal success.
    final Path secretFile = tempDir.resolve("secret.txt");
    Files.writeString(secretFile, "secret-content");
  }

  @Test
  void testFileIsServedCorrectly() throws Exception {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    exportResultProvider.result(TEST_JOB_ID, TEST_FILENAME, response);
    assertThat(response.getContentType()).isEqualTo("application/octet-stream");
    assertThat(response.containsHeader(HttpHeaders.CONTENT_DISPOSITION)).isTrue();
    assertThat(response.getHeader(HttpHeaders.CONTENT_DISPOSITION))
        .contains("attachment")
        .contains("filename=\"" + TEST_FILENAME + "\"");

    // Get the resource and read its content
    final String actualContent = response.getContentAsString();
    assertThat(actualContent).isNotEmpty().isEqualTo(TEST_CONTENT);
  }

  @Test
  void testNothingIsServedIfJobIsUnknown() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(
            () ->
                exportResultProvider.result(
                    "96f7f71b-f37e-4f1c-8cb4-0571e4b7f37b", TEST_FILENAME, response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessage("Unknown job id.");
  }

  @Test
  void testNothingIsServedIfFilenameIsUnknown() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "unknown-file.ndjson", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  // -------------------------------------------------------------------------
  // Path traversal tests
  // -------------------------------------------------------------------------

  @Test
  void testTraversalToParentDirectoryIsRejected() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "../../secret.txt", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testTraversalWithUrlEncodingIsRejected() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "..%2f..%2fsecret.txt", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testTraversalWithBackslashesIsRejected() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "..\\..\\secret.txt", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testAbsolutePathIsRejected() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "/etc/passwd", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testNullByteInjectionIsRejected() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(
            () -> exportResultProvider.result(TEST_JOB_ID, "test.txt\0../../etc/passwd", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testNestedTraversalEscapingJobDirIsRejected() {
    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(
            () -> exportResultProvider.result(TEST_JOB_ID, "subdir/../../../secret.txt", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testSymlinkEscapingJobDirIsRejected() throws Exception {
    // Create a sensitive target file outside the jobs hierarchy.
    final Path outside = tempDir.resolve("outside-secret.txt");
    Files.writeString(outside, "secret content");

    // Place a symlink inside the job directory that points to the outside file.
    final Path jobDir = tempDir.resolve("jobs").resolve(TEST_JOB_ID);
    final Path link = jobDir.resolve("escape.ndjson");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (final UnsupportedOperationException | IOException e) {
      // Symlink creation is unsupported on this filesystem; skip.
      return;
    }

    final MockHttpServletResponse response = new MockHttpServletResponse();
    assertThatCode(() -> exportResultProvider.result(TEST_JOB_ID, "escape.ndjson", response))
        .isExactlyInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("does not exist or is not a file.");
  }

  @Test
  void testValidNestedFileIsServed() throws Exception {
    // Create a nested file within the job directory.
    final Path jobDir = tempDir.resolve("jobs").resolve(TEST_JOB_ID);
    final Path subDir = jobDir.resolve("subdir");
    Files.createDirectories(subDir);
    final Path nestedFile = subDir.resolve("nested-file.ndjson");
    final String nestedContent = "{\"resourceType\":\"Patient\",\"id\":\"nested\"}";
    Files.writeString(nestedFile, nestedContent);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    exportResultProvider.result(TEST_JOB_ID, "subdir/nested-file.ndjson", response);
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getContentAsString()).isEqualTo(nestedContent);
  }
}

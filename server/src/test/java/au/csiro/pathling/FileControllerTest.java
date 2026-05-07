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

package au.csiro.pathling;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.ResponseEntity;

/**
 * Unit tests for {@link FileController}, focusing on path confinement to prevent directory
 * traversal attacks.
 *
 * @author John Grimes
 */
class FileControllerTest {

  /**
   * Tests that a legitimate file within the job directory is served successfully with the correct
   * response code and content disposition header.
   */
  @Test
  void servesFileWithinJobDirectory(@TempDir final Path tempDir) throws IOException {
    final Path jobsDir = tempDir.resolve("jobs").resolve("abc123");
    Files.createDirectories(jobsDir);
    final Path file = jobsDir.resolve("Patient.0000.ndjson");
    Files.writeString(file, "test content");

    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("abc123", "Patient.0000.ndjson");

    assertThat(response.getStatusCode().value()).isEqualTo(200);
    assertThat(response.getHeaders().getFirst("Content-Disposition"))
        .contains("attachment; filename=\"Patient.0000.ndjson\"");
  }

  /**
   * Tests that a filename containing path traversal sequences that would escape the job directory
   * returns HTTP 404.
   */
  @Test
  void rejectsFilenameWithPathTraversal(@TempDir final Path tempDir) {
    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("abc123", "../../etc/passwd");

    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }

  /**
   * Tests that a jobId containing path traversal sequences that would escape the jobs directory
   * returns HTTP 404.
   */
  @Test
  void rejectsJobIdWithPathTraversal(@TempDir final Path tempDir) {
    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("../secret", "loot");

    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }

  /**
   * Tests that a request for a path that resolves to a directory rather than a regular file returns
   * HTTP 404.
   */
  @Test
  void rejectsDirectoryRequest(@TempDir final Path tempDir) throws IOException {
    final Path jobsDir = tempDir.resolve("jobs").resolve("abc123");
    Files.createDirectories(jobsDir.resolve("secret.0000.ndjson"));

    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("abc123", "secret.0000.ndjson");

    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }

  /** Tests that a request for a non-existent file within a valid job directory returns HTTP 404. */
  @Test
  void rejectsNonExistentFile(@TempDir final Path tempDir) throws IOException {
    final Path jobsDir = tempDir.resolve("jobs").resolve("abc123");
    Files.createDirectories(jobsDir);

    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("abc123", "missing.ndjson");

    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }

  /**
   * Tests that a regular file inside the job directory which is a symlink to a target outside the
   * jobs hierarchy returns HTTP 404. A purely lexical {@code startsWith} check would incorrectly
   * permit this; filesystem-aware resolution must be used.
   */
  @Test
  void rejectsSymlinkPointingOutsideJobsDirectory(@TempDir final Path tempDir) throws IOException {
    final Path jobDir = tempDir.resolve("jobs").resolve("abc123");
    Files.createDirectories(jobDir);

    // Create a sensitive target file outside the jobs hierarchy.
    final Path outside = tempDir.resolve("outside.txt");
    Files.writeString(outside, "secret content");

    // Place a symlink inside the job directory that points to the outside file.
    final Path link = jobDir.resolve("escape.ndjson");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (final UnsupportedOperationException | IOException e) {
      // Symlink creation is unsupported on this filesystem (e.g. Windows without privileges).
      // Skip the test in that environment.
      return;
    }

    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("abc123", "escape.ndjson");

    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }

  /**
   * Tests that when the per-job directory itself is a symlink pointing outside the jobs hierarchy,
   * requests for files under it are rejected with HTTP 404.
   */
  @Test
  void rejectsJobDirectorySymlinkedOutside(@TempDir final Path tempDir) throws IOException {
    final Path jobsDir = tempDir.resolve("jobs");
    Files.createDirectories(jobsDir);

    // Create a directory outside the jobs hierarchy with a regular file inside.
    final Path outsideDir = tempDir.resolve("outside-dir");
    Files.createDirectories(outsideDir);
    Files.writeString(outsideDir.resolve("loot.txt"), "secret content");

    // Symlink the per-job directory to the outside directory.
    final Path linkedJobDir = jobsDir.resolve("abc123");
    try {
      Files.createSymbolicLink(linkedJobDir, outsideDir);
    } catch (final UnsupportedOperationException | IOException e) {
      return;
    }

    final FileController controller = new FileController(tempDir.toUri().toString());
    final ResponseEntity<?> response = controller.serveFile("abc123", "loot.txt");

    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }
}

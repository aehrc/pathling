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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for serving job output files.
 *
 * @author Felix Naumann
 */
@RestController
@Slf4j
public class FileController {

  private final String databasePath;

  /**
   * Creates a new FileController.
   *
   * @param databasePath the path to the database directory
   */
  public FileController(
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath) {
    this.databasePath = databasePath;
  }

  /**
   * Serves a file from a job's output directory.
   *
   * @param jobId the job identifier
   * @param filename the name of the file to serve
   * @return the file content as a response entity
   */
  @GetMapping("/jobs/{jobId}/{filename}")
  public ResponseEntity<Resource> serveFile(
      @PathVariable("jobId") final String jobId, @PathVariable("filename") final String filename) {

    try {
      final Path basePath =
          Paths.get(URI.create(databasePath).getPath()).toAbsolutePath().normalize();
      final Path jobsDir = basePath.resolve("jobs").normalize().toAbsolutePath();
      final Path jobDir = jobsDir.resolve(jobId).normalize().toAbsolutePath();
      final Path requestedFile = jobDir.resolve(filename).normalize().toAbsolutePath();

      // Validate that the job directory remains within the jobs directory,
      // and that the requested file remains within the job directory.
      if (!jobDir.startsWith(jobsDir) || !requestedFile.startsWith(jobDir)) {
        return ResponseEntity.notFound().build();
      }

      if (!Files.isRegularFile(requestedFile)) {
        return ResponseEntity.notFound().build();
      }

      final Resource resource = new FileSystemResource(requestedFile.toString());
      return ResponseEntity.ok()
          .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
          .body(resource);
    } catch (final Exception e) {
      // Fail closed on any unexpected error (e.g. malformed databasePath, invalid path
      // characters). Log at debug to aid operator diagnosis without leaking detail to the client.
      log.debug("Failed to serve file for jobId={}, filename={}", jobId, filename, e);
      return ResponseEntity.notFound().build();
    }
  }
}

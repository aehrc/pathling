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

import au.csiro.pathling.io.JobDirectoryFileSystem;
import au.csiro.pathling.io.JobFile;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
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
 * @author John Grimes
 */
@RestController
@Slf4j
public class FileController {

  @Nonnull private final JobDirectoryFileSystem jobDirectoryFileSystem;

  /**
   * Creates a new FileController.
   *
   * @param jobDirectoryFileSystem the helper used to access the per-job directory under the
   *     warehouse via the Hadoop {@link org.apache.hadoop.fs.FileSystem} API
   */
  public FileController(@Nonnull final JobDirectoryFileSystem jobDirectoryFileSystem) {
    this.jobDirectoryFileSystem = jobDirectoryFileSystem;
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

    final Optional<JobFile> opened = jobDirectoryFileSystem.openForRead(jobId, filename);
    if (opened.isEmpty()) {
      return ResponseEntity.notFound().build();
    }

    final JobFile jobFile = opened.orElseThrow();
    final Resource resource = new InputStreamResource(jobFile.getStream());
    return ResponseEntity.ok()
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
        .contentLength(jobFile.getLength())
        .body(resource);
  }
}

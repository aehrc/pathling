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

package au.csiro.pathling.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DirectoryCleanup}, the teardown helper that tolerates entries being deleted
 * concurrently by the server (see issue #2711).
 *
 * @author John Grimes
 */
class DirectoryCleanupTest {

  @TempDir private Path tempDir;

  @Test
  void deletesNestedContentsAndPreservesRoot() throws IOException {
    // Build a structure with nested directories and files, mirroring a jobs directory containing
    // job subdirectories with output files.
    final Path jobDir = Files.createDirectories(tempDir.resolve("job-1").resolve("output"));
    Files.writeString(jobDir.resolve("result.ndjson"), "{}");
    Files.writeString(tempDir.resolve("stray-file.txt"), "stray");
    Files.createDirectory(tempDir.resolve("empty-dir"));

    DirectoryCleanup.cleanDirectoryTolerantly(tempDir);

    // The root must survive, but all of its contents must be gone.
    assertThat(tempDir).isEmptyDirectory();
  }

  @Test
  void missingDirectoryIsANoOp() {
    // A directory that has already been deleted underneath us is a normal outcome, not a failure.
    // This exercises the same tolerance branch that a mid-walk concurrent deletion hits.
    final Path missing = tempDir.resolve("does-not-exist");

    assertThatCode(() -> DirectoryCleanup.cleanDirectoryTolerantly(missing))
        .doesNotThrowAnyException();
  }

  @Test
  void emptyDirectoryIsANoOp() {
    assertThatCode(() -> DirectoryCleanup.cleanDirectoryTolerantly(tempDir))
        .doesNotThrowAnyException();

    assertThat(tempDir).exists();
  }
}

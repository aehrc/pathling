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

package au.csiro.pathling.operations.export;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.io.JobDirectoryFileSystem;
import au.csiro.pathling.io.StubFileSystem;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for {@link ExportFileWriter}'s per-job directory handling on a warehouse whose
 * filesystem scheme differs from {@code fs.defaultFS}.
 *
 * <p>Against the pre-fix writer (which resolved the filesystem with {@code
 * FileSystem.get(configuration)} and then operated on a warehouse-scheme path) {@code
 * createJobDirectory} failed with {@code IllegalArgumentException: Wrong FS} - exactly the
 * production defect. The writer now delegates to {@link JobDirectoryFileSystem}, which resolves the
 * filesystem from the warehouse URI, so the operation succeeds on any scheme.
 *
 * <p>The Spark session is mocked because per-job directory creation and deletion no longer touch
 * Spark; the dataset-write methods that do use it are covered by the executor tests.
 *
 * @author John Grimes
 */
class ExportFileWriterTest {

  /** A Hadoop configuration with a {@code file:///} default that can also resolve {@code stub}. */
  private static Configuration stubSchemeConfiguration() {
    final Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "file:///");
    configuration.setClass("fs.stub.impl", StubFileSystem.class, FileSystem.class);
    return configuration;
  }

  private static ExportFileWriter newWriter(final Path tempDir) {
    final URI warehouseUri = URI.create("stub://" + tempDir.toAbsolutePath() + "/default");
    final JobDirectoryFileSystem helper =
        new JobDirectoryFileSystem(warehouseUri, stubSchemeConfiguration());
    return new ExportFileWriter(mock(SparkSession.class), helper);
  }

  @Test
  void createJobDirectoryResolvesWarehouseSchemeAndCreatesDirectory(@TempDir final Path tempDir) {
    final ExportFileWriter writer = newWriter(tempDir);

    final org.apache.hadoop.fs.Path jobDir = writer.createJobDirectory("job1");

    // The returned path must carry the warehouse scheme, not the default file:// scheme.
    assertThat(jobDir.toUri().getScheme()).isEqualTo("stub");
    // The stub filesystem stores data on the local disk keyed on the path portion.
    assertThat(tempDir.resolve("default").resolve("jobs").resolve("job1")).isDirectory();
  }

  @Test
  void deleteJobDirectoryRemovesDirectoryOnWarehouseScheme(@TempDir final Path tempDir) {
    final ExportFileWriter writer = newWriter(tempDir);
    writer.createJobDirectory("job1");

    writer.deleteJobDirectory("job1");

    assertThat(tempDir.resolve("default").resolve("jobs").resolve("job1")).doesNotExist();
  }

  @Test
  void createJobDirectoryWrapsHelperFailureAsInternalError() throws IOException {
    // A failure to create the directory must surface as an InternalErrorException, not the raw
    // IOException.
    final JobDirectoryFileSystem failingHelper = mock(JobDirectoryFileSystem.class);
    doThrow(new IOException("boom")).when(failingHelper).ensureJobDirectory("job1");
    final ExportFileWriter writer = new ExportFileWriter(mock(SparkSession.class), failingHelper);

    assertThatThrownBy(() -> writer.createJobDirectory("job1"))
        .isInstanceOf(InternalErrorException.class);
  }

  @Test
  void deleteJobDirectorySwallowsFailures() throws IOException {
    // Cleanup of a partial output directory is best-effort: an underlying failure must be logged
    // and swallowed so it never masks the original export error.
    final JobDirectoryFileSystem failingHelper = mock(JobDirectoryFileSystem.class);
    doThrow(new IOException("boom")).when(failingHelper).deleteJobDirectory("job1");
    final ExportFileWriter writer = new ExportFileWriter(mock(SparkSession.class), failingHelper);

    assertThatCode(() -> writer.deleteJobDirectory("job1")).doesNotThrowAnyException();
  }
}

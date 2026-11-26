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

package au.csiro.pathling.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link JobDirectoryFileSystem}, exercising path validation, containment, and
 * symlink-aware behaviour against a local {@code file://} warehouse.
 *
 * @author John Grimes
 */
class JobDirectoryFileSystemTest {

  /** Constructs a helper rooted at {@code tempDir} with a default Hadoop configuration. */
  private static JobDirectoryFileSystem newHelper(final Path tempDir) {
    return new JobDirectoryFileSystem(tempDir.toUri(), new Configuration());
  }

  // -------------------------------------------------------------------------
  // Path validation
  // -------------------------------------------------------------------------

  @Test
  void rejectsParentTraversalInJobId(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("..", "file.ndjson"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsParentTraversalSegmentInFilename(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "../escape.txt"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAbsolutePathFilename(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "/etc/passwd"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsBackslashInFilename(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "..\\escape.txt"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNullByteInFilename(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "a.txt\u0000b"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsEmptyJobId(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("", "file.ndjson"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNestedParentTraversal(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "subdir/../../escape.txt"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // A job identifier names a directory directly beneath the jobs root, so it must be a single plain
  // path segment. A separator, a scheme or a leading dot would let one identifier resolve to the
  // directory of another job, or to a location outside the warehouse.
  @ParameterizedTest
  @ValueSource(
      strings = {
        ".",
        "./job1",
        ".hidden",
        "job1/.",
        "a/b",
        "other/../job1",
        "file:/tmp/escape",
        "file:job1",
        "s3a://bucket/job1"
      })
  void rejectsJobIdThatIsNotAPlainSegment(final String jobId, @TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile(jobId, "file.ndjson"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> helper.jobDirectory(jobId))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> helper.openForWrite(jobId, "file.ndjson", true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> helper.deleteJobDirectory(jobId))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // A dot-prefixed alias of an existing job directory must not give access to that job's files.
  @Test
  void openForReadReturnsEmptyForAliasOfExistingJob(@TempDir final Path tempDir)
      throws IOException {
    final Path jobDir = tempDir.resolve("jobs").resolve("job1");
    Files.createDirectories(jobDir);
    Files.writeString(jobDir.resolve("data.ndjson"), "secret");

    final JobDirectoryFileSystem helper = newHelper(tempDir);

    assertThat(helper.openForRead("./job1", "data.ndjson")).isEmpty();
    // The control: the same file is readable through the job's own identifier.
    assertThat(helper.openForRead("job1", "data.ndjson")).isPresent();
  }

  // A scheme-qualified job identifier must not create a directory anywhere, in particular not at
  // the location the scheme points to outside the warehouse.
  @Test
  void ensureJobDirectoryCreatesNothingForSchemeQualifiedJobId(@TempDir final Path tempDir) {
    final Path outside = tempDir.resolve("escape");
    final JobDirectoryFileSystem helper = newHelper(tempDir.resolve("warehouse"));

    assertThatThrownBy(() -> helper.ensureJobDirectory("file:" + outside))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(outside).doesNotExist();
  }

  @Test
  void resolvesValidJobIdAndFilename(@TempDir final Path tempDir) throws IOException {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    final org.apache.hadoop.fs.Path resolved = helper.resolveFile("job1", "Patient.0000.ndjson");
    assertThat(resolved.toString()).endsWith("/jobs/job1/Patient.0000.ndjson");
  }

  @Test
  void rejectsFilenameWithSeparator(@TempDir final Path tempDir) {
    // A filename names a file directly inside the job directory, never a nested path.
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "subdir/Patient.0000.ndjson"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsFilenameWithScheme(@TempDir final Path tempDir) {
    // Hadoop resolves a child path carrying a scheme to that absolute location, ignoring the
    // parent, so a filename with a colon could otherwise name any file.
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    final String otherJobFile = tempDir.resolve("jobs/job2/data.ndjson").toUri().toString();
    assertThatThrownBy(() -> helper.resolveFile("job1", otherJobFile))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsFilenameWithColon(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "file:data.ndjson"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsDotFilename(@TempDir final Path tempDir) {
    // A single dot names the job directory itself rather than a file within it.
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.resolveFile("job1", "."))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsJobIdResolvingOutsideJobsDirectory(@TempDir final Path tempDir) {
    // A job identifier carrying a scheme would resolve to an absolute location outside the jobs
    // directory, so it is rejected as an invalid identifier before any path is resolved.
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    final String outsideDir = tempDir.resolve("outside").toUri().toString();
    assertThatThrownBy(() -> helper.resolveFile(outsideDir, "data.ndjson"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------
  // Reading
  // -------------------------------------------------------------------------

  @Test
  void openForReadStreamsValidFile(@TempDir final Path tempDir) throws IOException {
    final Path jobDir = tempDir.resolve("jobs").resolve("job1");
    Files.createDirectories(jobDir);
    Files.writeString(jobDir.resolve("data.ndjson"), "hello world");

    final JobDirectoryFileSystem helper = newHelper(tempDir);
    final Optional<JobFile> opened = helper.openForRead("job1", "data.ndjson");

    assertThat(opened).isPresent();
    try (final FSDataInputStream stream = opened.orElseThrow().getStream()) {
      final String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(content).isEqualTo("hello world");
    }
    assertThat(opened.orElseThrow().getLength()).isEqualTo(11L);
  }

  @Test
  void openForReadReturnsEmptyForNonExistentFile(@TempDir final Path tempDir) throws IOException {
    Files.createDirectories(tempDir.resolve("jobs").resolve("job1"));
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThat(helper.openForRead("job1", "missing.ndjson")).isEmpty();
  }

  @Test
  void openForReadReturnsEmptyForDirectory(@TempDir final Path tempDir) throws IOException {
    Files.createDirectories(tempDir.resolve("jobs").resolve("job1").resolve("subdir"));
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThat(helper.openForRead("job1", "subdir")).isEmpty();
  }

  @Test
  void openForReadReturnsEmptyForFileSymlinkOutsideJobsDir(@TempDir final Path tempDir)
      throws IOException {
    final Path jobDir = tempDir.resolve("jobs").resolve("job1");
    Files.createDirectories(jobDir);
    final Path outside = tempDir.resolve("outside.txt");
    Files.writeString(outside, "secret");
    final Path link = jobDir.resolve("escape.ndjson");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (final UnsupportedOperationException | IOException e) {
      return;
    }

    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThat(helper.openForRead("job1", "escape.ndjson")).isEmpty();
  }

  @Test
  void openForReadReturnsEmptyForJobDirSymlinkOutsideJobsHierarchy(@TempDir final Path tempDir)
      throws IOException {
    Files.createDirectories(tempDir.resolve("jobs"));
    final Path outsideDir = tempDir.resolve("outside-dir");
    Files.createDirectories(outsideDir);
    Files.writeString(outsideDir.resolve("loot.txt"), "secret");
    final Path linkedJobDir = tempDir.resolve("jobs").resolve("job1");
    try {
      Files.createSymbolicLink(linkedJobDir, outsideDir);
    } catch (final UnsupportedOperationException | IOException e) {
      return;
    }

    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThat(helper.openForRead("job1", "loot.txt")).isEmpty();
  }

  @Test
  void openForReadReturnsEmptyForSchemeUriNamingAnotherJobsFile(@TempDir final Path tempDir)
      throws IOException {
    // A caller who passes the ownership check for job1 must not be able to read a file from job2's
    // directory by naming it with an absolute file: URI.
    Files.createDirectories(tempDir.resolve("jobs").resolve("job1"));
    final Path otherJobDir = tempDir.resolve("jobs").resolve("job2");
    Files.createDirectories(otherJobDir);
    final Path otherJobFile = otherJobDir.resolve("patients.00000.csv");
    Files.writeString(otherJobFile, "secret");

    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThat(helper.openForRead("job1", otherJobFile.toUri().toString())).isEmpty();
    assertThat(helper.openForRead("job1", "file:" + otherJobFile.toAbsolutePath())).isEmpty();
  }

  @Test
  void openForReadReturnsEmptyForFileSymlinkIntoAnotherJob(@TempDir final Path tempDir)
      throws IOException {
    // A symbolic link inside one job's directory must not expose a file from another job, even
    // though the target is still within the jobs directory.
    final Path jobDir = tempDir.resolve("jobs").resolve("job1");
    Files.createDirectories(jobDir);
    final Path otherJobDir = tempDir.resolve("jobs").resolve("job2");
    Files.createDirectories(otherJobDir);
    final Path otherJobFile = otherJobDir.resolve("data.ndjson");
    Files.writeString(otherJobFile, "secret");
    try {
      Files.createSymbolicLink(jobDir.resolve("link.ndjson"), otherJobFile);
    } catch (final UnsupportedOperationException | IOException e) {
      return;
    }

    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThat(helper.openForRead("job1", "link.ndjson")).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Writing
  // -------------------------------------------------------------------------

  @Test
  void ensureJobDirectoryCreatesNestedDirectories(@TempDir final Path tempDir) throws IOException {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    helper.ensureJobDirectory("brand-new-job");

    assertThat(tempDir.resolve("jobs").resolve("brand-new-job")).isDirectory();
  }

  @Test
  void openForWriteWritesValidFile(@TempDir final Path tempDir) throws IOException {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    helper.ensureJobDirectory("job1");

    try (final OutputStream out = helper.openForWrite("job1", "data.ndjson", true)) {
      out.write("written".getBytes(StandardCharsets.UTF_8));
    }

    final Path written = tempDir.resolve("jobs").resolve("job1").resolve("data.ndjson");
    assertThat(Files.readString(written)).isEqualTo("written");
  }

  @Test
  void openForWriteRejectsTraversal(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.openForWrite("job1", "../escape.txt", true))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------
  // Deletion
  // -------------------------------------------------------------------------

  @Test
  void deleteJobDirectoryRemovesExistingDirectoryRecursively(@TempDir final Path tempDir)
      throws IOException {
    final Path jobDir = tempDir.resolve("jobs").resolve("job1");
    Files.createDirectories(jobDir.resolve("subdir"));
    Files.writeString(jobDir.resolve("data.ndjson"), "content");
    Files.writeString(jobDir.resolve("subdir").resolve("more.ndjson"), "more");

    final JobDirectoryFileSystem helper = newHelper(tempDir);
    helper.deleteJobDirectory("job1");

    assertThat(jobDir).doesNotExist();
    // The jobs root itself is left in place; only the per-job directory is removed.
    assertThat(tempDir.resolve("jobs")).isDirectory();
  }

  @Test
  void deleteJobDirectoryIsNoOpWhenAbsent(@TempDir final Path tempDir) throws IOException {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    // Deleting a directory that was never created must complete without error.
    helper.deleteJobDirectory("never-created");
    assertThat(tempDir.resolve("jobs").resolve("never-created")).doesNotExist();
  }

  @Test
  void deleteJobDirectoryRejectsTraversalId(@TempDir final Path tempDir) {
    final JobDirectoryFileSystem helper = newHelper(tempDir);
    assertThatThrownBy(() -> helper.deleteJobDirectory(".."))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // -------------------------------------------------------------------------
  // Non-default warehouse filesystem scheme (regression for Wrong FS)
  // -------------------------------------------------------------------------

  /**
   * Builds a Hadoop configuration whose default filesystem is {@code file:///} but which also knows
   * how to resolve the {@code stub} scheme, mirroring a server whose warehouse is on a non-default
   * filesystem.
   */
  private static Configuration stubSchemeConfiguration() {
    final Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "file:///");
    configuration.setClass(
        "fs.stub.impl", StubFileSystem.class, org.apache.hadoop.fs.FileSystem.class);
    return configuration;
  }

  /** Returns a helper rooted at {@code tempDir} but addressed through the {@code stub} scheme. */
  private static JobDirectoryFileSystem newStubHelper(final Path tempDir) {
    final URI stubDatabaseUri = URI.create("stub://" + tempDir.toAbsolutePath() + "/default");
    return new JobDirectoryFileSystem(stubDatabaseUri, stubSchemeConfiguration());
  }

  @Test
  void jobDirectoryResolvesStubSchemeWhenDefaultIsFile(@TempDir final Path tempDir)
      throws IOException {
    // The resolved job directory must carry the warehouse scheme, not the default file:// scheme.
    final org.apache.hadoop.fs.Path jobDir = newStubHelper(tempDir).jobDirectory("job1");
    assertThat(jobDir.toUri().getScheme()).isEqualTo("stub");
    assertThat(jobDir.toString()).endsWith("/default/jobs/job1");
  }

  @Test
  void ensureAndDeleteJobDirectoryOperateOnStubScheme(@TempDir final Path tempDir)
      throws IOException {
    // Proves the helper resolves the filesystem from the warehouse URI, not the process default.
    // The physical files land under the temp directory because the stub filesystem stores data on
    // the local disk keyed on the path portion.
    final JobDirectoryFileSystem helper = newStubHelper(tempDir);
    final Path physicalJobDir = tempDir.resolve("default").resolve("jobs").resolve("job1");

    helper.ensureJobDirectory("job1");
    assertThat(physicalJobDir).isDirectory();

    helper.deleteJobDirectory("job1");
    assertThat(physicalJobDir).doesNotExist();
  }

  // -------------------------------------------------------------------------
  // Warehouse URI composition
  // -------------------------------------------------------------------------

  @Test
  void trailingSlashWarehouseUrlResolvesSameJobDirectory(@TempDir final Path tempDir)
      throws IOException {
    // A warehouseUrl configured with a trailing slash composes a database URI containing a
    // redundant separator (e.g. .../warehouse//default). Path normalisation must collapse it so
    // the resolved job directory is identical to the form without the trailing slash, and contains
    // no empty segment, which some object stores would treat as a distinct key.
    final String base = tempDir.toUri().toString().replaceAll("/+$", "");
    final JobDirectoryFileSystem withSlash =
        new JobDirectoryFileSystem(URI.create(base + "//default"), new Configuration());
    final JobDirectoryFileSystem withoutSlash =
        new JobDirectoryFileSystem(URI.create(base + "/default"), new Configuration());

    final String resolved = withSlash.jobDirectory("job1").toString();

    assertThat(resolved).isEqualTo(withoutSlash.jobDirectory("job1").toString());
    assertThat(resolved).endsWith("/default/jobs/job1");
    // The scheme is the only place a double slash may legitimately appear (e.g. s3a://); the local
    // file scheme renders without one, so the resolved path must contain no empty segment at all.
    assertThat(resolved).doesNotContain("//");
  }
}

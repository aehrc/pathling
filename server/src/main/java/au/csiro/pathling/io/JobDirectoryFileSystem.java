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

import jakarta.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * File-system access helper for the per-job directory under the warehouse, using the Hadoop {@link
 * FileSystem} API so that the same code path works for {@code file://}, {@code s3a://}, {@code
 * hdfs://}, and any other Spark-supported warehouse scheme.
 *
 * <p>All resolution goes through validated path construction that rejects path traversal (parent
 * segments, absolute paths, null bytes, backslashes) and verifies containment within the canonical
 * jobs directory. For the local {@code file://} scheme, symbolic links are also resolved before the
 * containment check, defeating symlink escapes.
 *
 * @author John Grimes
 */
@Component
@Slf4j
public class JobDirectoryFileSystem {

  @Nonnull private static final String JOBS_SUBDIRECTORY = "jobs";

  @Nonnull private final URI databaseUri;

  @Nonnull private final Configuration hadoopConfiguration;

  /**
   * Spring constructor.
   *
   * @param databasePath the warehouse + database URL (Hadoop-compatible)
   * @param hadoopConfiguration the Hadoop configuration
   */
  @Autowired
  public JobDirectoryFileSystem(
      @Nonnull @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath,
      @Nonnull final Configuration hadoopConfiguration) {
    this(URI.create(databasePath), hadoopConfiguration);
  }

  /**
   * Programmatic constructor used by tests and call sites that already hold a parsed URI.
   *
   * @param databaseUri the URI of the database directory under the warehouse
   * @param hadoopConfiguration the Hadoop configuration
   */
  public JobDirectoryFileSystem(
      @Nonnull final URI databaseUri, @Nonnull final Configuration hadoopConfiguration) {
    this.databaseUri = databaseUri;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  /**
   * Returns the Hadoop {@link FileSystem} rooted at the warehouse URI.
   *
   * @return the file system
   * @throws IOException if the file system cannot be obtained
   */
  @Nonnull
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(databaseUri, hadoopConfiguration);
  }

  /**
   * Returns the qualified Hadoop {@link Path} of the per-job directory. Performs no I/O; the
   * directory may or may not yet exist.
   *
   * @param jobId the job identifier
   * @return the qualified path of the per-job directory
   * @throws IllegalArgumentException if the job identifier contains traversal sequences
   * @throws IOException if the file system cannot be obtained
   */
  @Nonnull
  public Path jobDirectory(@Nonnull final String jobId) throws IOException {
    rejectInvalidComponent(jobId);
    return getFileSystem().makeQualified(new Path(jobsDirectoryRaw(), jobId));
  }

  /**
   * Resolves a filename within the per-job directory and validates that the resulting qualified
   * path lies inside the jobs directory.
   *
   * @param jobId the job identifier
   * @param filename the requested filename
   * @return the qualified Hadoop path of the requested file
   * @throws IllegalArgumentException if the jobId or filename contains traversal sequences
   * @throws IOException if the resolved path falls outside the jobs directory
   */
  @Nonnull
  public Path resolveFile(@Nonnull final String jobId, @Nonnull final String filename)
      throws IOException {
    rejectInvalidComponent(jobId);
    rejectInvalidComponent(filename);
    final FileSystem fs = getFileSystem();
    final Path qualifiedJobsDir = fs.makeQualified(jobsDirectoryRaw());
    final Path resolved = fs.makeQualified(new Path(new Path(qualifiedJobsDir, jobId), filename));
    if (!hasPathPrefix(resolved.toString(), qualifiedJobsDir.toString())) {
      throw new IOException("Resolved path falls outside the jobs directory: " + resolved);
    }
    return resolved;
  }

  /**
   * Ensures the per-job directory exists, creating it (and parents) if necessary.
   *
   * @param jobId the job identifier
   * @throws IOException if the directory could not be created
   */
  public void ensureJobDirectory(@Nonnull final String jobId) throws IOException {
    final FileSystem fs = getFileSystem();
    final Path jobDir = jobDirectory(jobId);
    if (!fs.exists(jobDir) && !fs.mkdirs(jobDir)) {
      throw new IOException("Failed to create job directory: " + jobDir);
    }
  }

  /**
   * Resolves a file for reading, validating containment, existence and (for {@code file://}
   * schemes) symlink-aware containment, then opens an input stream.
   *
   * <p>The caller owns the returned {@link FSDataInputStream} and is responsible for closing it.
   *
   * @param jobId the job identifier
   * @param filename the requested filename
   * @return a {@link JobFile} carrying the qualified path, opened input stream and length, or empty
   *     if any check fails
   */
  @Nonnull
  public Optional<JobFile> openForRead(
      @Nonnull final String jobId, @Nonnull final String filename) {
    final FileSystem fs;
    final Path resolved;
    try {
      fs = getFileSystem();
      resolved = resolveFile(jobId, filename);
    } catch (final IllegalArgumentException | IOException e) {
      log.debug("Rejected job file read: jobId={}, filename={}", jobId, filename, e);
      return Optional.empty();
    }

    final FileStatus status;
    try {
      status = fs.getFileStatus(resolved);
    } catch (final FileNotFoundException e) {
      return Optional.empty();
    } catch (final IOException e) {
      log.debug("Failed to stat job file: jobId={}, filename={}", jobId, filename, e);
      return Optional.empty();
    }
    if (!status.isFile()) {
      return Optional.empty();
    }

    // For local file scheme, defeat symlink escapes by verifying canonical containment.
    if ("file".equals(resolved.toUri().getScheme())) {
      try {
        if (!isSymlinkContained(resolved)) {
          return Optional.empty();
        }
      } catch (final IOException e) {
        log.debug("Failed to canonicalise job file: jobId={}, filename={}", jobId, filename, e);
        return Optional.empty();
      }
    }

    try {
      final FSDataInputStream stream = fs.open(resolved);
      return Optional.of(new JobFile(resolved, stream, status.getLen()));
    } catch (final IOException e) {
      log.debug("Failed to open job file: jobId={}, filename={}", jobId, filename, e);
      return Optional.empty();
    }
  }

  /**
   * Opens a writable {@link OutputStream} for a file inside the per-job directory.
   *
   * <p>The caller owns the stream and is responsible for closing it.
   *
   * @param jobId the job identifier
   * @param filename the target filename within the per-job directory
   * @param overwrite whether to overwrite an existing file at the same path
   * @return an opened output stream
   * @throws IOException if the path cannot be resolved or the stream cannot be opened
   */
  @Nonnull
  public OutputStream openForWrite(
      @Nonnull final String jobId, @Nonnull final String filename, final boolean overwrite)
      throws IOException {
    final FileSystem fs = getFileSystem();
    final Path resolved = resolveFile(jobId, filename);
    return fs.create(resolved, overwrite);
  }

  /**
   * Checks the canonical (symlink-resolved) containment of a {@code file://} path within the jobs
   * directory.
   */
  private boolean isSymlinkContained(@Nonnull final Path resolved) throws IOException {
    final java.nio.file.Path realFile = java.nio.file.Path.of(resolved.toUri()).toRealPath();
    final Path qualifiedJobsDir = getFileSystem().makeQualified(jobsDirectoryRaw());
    final java.nio.file.Path realJobsDir =
        java.nio.file.Path.of(qualifiedJobsDir.toUri()).toRealPath();
    return realFile.startsWith(realJobsDir);
  }

  /** Returns the jobs directory as a Hadoop path; not yet qualified by the file system. */
  @Nonnull
  private Path jobsDirectoryRaw() {
    return new Path(new Path(databaseUri), JOBS_SUBDIRECTORY);
  }

  /**
   * Tests whether {@code candidate} is within {@code prefix} by performing a directory-aware string
   * prefix check. Adds a trailing separator to the prefix so that {@code /a/jobs} does not match
   * {@code /a/jobs-other/x}.
   */
  private static boolean hasPathPrefix(
      @Nonnull final String candidate, @Nonnull final String prefix) {
    final String normalisedPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
    return candidate.equals(prefix) || candidate.startsWith(normalisedPrefix);
  }

  /** Rejects empty components and components that would enable traversal beyond the jobs root. */
  private static void rejectInvalidComponent(@Nonnull final String component) {
    if (component.isEmpty()
        || component.contains("\u0000")
        || component.contains("\\")
        || component.startsWith("/")
        || "..".equals(component)
        || component.startsWith("../")
        || component.endsWith("/..")
        || component.contains("/../")) {
      throw new IllegalArgumentException("Invalid path component: " + component);
    }
  }
}

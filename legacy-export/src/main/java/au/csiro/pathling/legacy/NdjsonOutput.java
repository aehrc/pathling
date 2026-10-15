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
package au.csiro.pathling.legacy;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;

/**
 * Writes FHIR JSON documents to files of newline-delimited JSON named for their resource type, as
 * {@code <type>.<part>.ndjson}, which is the naming the NDJSON source reads a directory by.
 *
 * <p>Spark writes text as a directory of part files, so the documents are written to a staging
 * directory within the output and each part file is then moved beside it under its final name. The
 * staging directory's name begins with an underscore, which Spark's readers skip, and it is removed
 * whether or not the write succeeds.
 *
 * @author Piotr Szul
 */
@Slf4j
final class NdjsonOutput {

  @Nonnull private static final String EXTENSION = ".ndjson";

  @Nonnull private static final String PART_PREFIX = "part-";

  private NdjsonOutput() {}

  /**
   * Writes documents of one resource type into a directory.
   *
   * @param documents the documents, one per resource
   * @param resourceType the resource type
   * @param outputDirectory the directory to write into, which is created if it does not exist
   * @return the paths of the files written
   * @throws LegacyExportException if the directory already holds files of the resource type
   */
  @Nonnull
  static List<String> write(
      @Nonnull final Dataset<String> documents,
      @Nonnull final String resourceType,
      @Nonnull final String outputDirectory) {
    final Path output = new Path(outputDirectory);
    try {
      final FileSystem fileSystem =
          output.getFileSystem(documents.sparkSession().sparkContext().hadoopConfiguration());
      refuseExisting(fileSystem, output, resourceType);
      final Path staging = new Path(output, "_" + resourceType + "-" + UUID.randomUUID());
      try {
        documents.write().text(staging.toString());
        return moveParts(fileSystem, staging, output, resourceType);
      } finally {
        removeStaging(fileSystem, staging);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Removes the staging directory. A failure to remove it is logged rather than thrown, so that it
   * cannot hide the failure of the write it follows.
   */
  private static void removeStaging(
      @Nonnull final FileSystem fileSystem, @Nonnull final Path staging) {
    try {
      fileSystem.delete(staging, true);
    } catch (final IOException e) {
      log.warn("Unable to remove the staging directory {}", staging, e);
    }
  }

  private static void refuseExisting(
      @Nonnull final FileSystem fileSystem,
      @Nonnull final Path output,
      @Nonnull final String resourceType)
      throws IOException {
    final FileStatus[] existing =
        fileSystem.globStatus(new Path(output, resourceType + "{,.*}" + EXTENSION));
    if (existing != null && existing.length > 0) {
      throw new LegacyExportException(
          "The output "
              + output
              + " already holds "
              + resourceType
              + " files, such as "
              + existing[0].getPath().getName()
              + ". Export into a directory that holds no "
              + resourceType
              + " files, or remove them first.");
    }
  }

  @Nonnull
  private static List<String> moveParts(
      @Nonnull final FileSystem fileSystem,
      @Nonnull final Path staging,
      @Nonnull final Path output,
      @Nonnull final String resourceType)
      throws IOException {
    final List<Path> parts =
        Arrays.stream(fileSystem.listStatus(staging))
            .map(FileStatus::getPath)
            .filter(path -> path.getName().startsWith(PART_PREFIX))
            .sorted(Comparator.comparing(Path::getName))
            .toList();
    final List<String> written = new ArrayList<>(parts.size());
    for (int index = 0; index < parts.size(); index++) {
      final Path target =
          new Path(output, String.format("%s.%05d%s", resourceType, index, EXTENSION));
      if (!fileSystem.rename(parts.get(index), target)) {
        throw new IOException("Unable to move " + parts.get(index) + " to " + target);
      }
      written.add(target.toString());
    }
    return written;
  }
}

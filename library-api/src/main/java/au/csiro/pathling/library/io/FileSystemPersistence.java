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

package au.csiro.pathling.library.io;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.io.source.FileSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

/**
 * Utility methods relating to persisting data to the filesystem.
 *
 * @author John Grimes
 */
@Slf4j
public class FileSystemPersistence {

  /**
   * The name Spark gives a file it writes: the partition id and the id of the write, then an
   * optional file counter and the extensions.
   */
  @Nonnull
  private static final Pattern SPARK_PART_FILE =
      Pattern.compile(
          "^part-(\\d+)-([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})(?:[-.].*)?$");

  private FileSystemPersistence() {}

  /**
   * Joins two paths together, taking care that URLs, Unix-style paths and Windows-style paths are
   * catered for.
   *
   * @param first the first path
   * @param second the second path
   * @return the joined path
   */
  @Nonnull
  public static String safelyJoinPaths(@Nonnull final String first, @Nonnull final String second) {
    try {
      final URI uri = URI.create(first);
      return uri.toString().replaceFirst("/$", "") + "/" + second;
    } catch (final IllegalArgumentException e) {
      return java.nio.file.Path.of(first, second).toString();
    }
  }

  /**
   * Get a Hadoop {@link FileSystem} for the given location.
   *
   * @param spark the Spark session
   * @param location the location URL to be accessed
   * @return the {@link FileSystem} for the given location
   * @throws PersistenceError if there is a problem accessing the location
   * @throws IllegalArgumentException if the location URL is malformed
   */
  @Nonnull
  public static FileSystem getFileSystem(
      @Nonnull final SparkSession spark, @Nonnull final String location) {
    @Nullable
    final org.apache.hadoop.conf.Configuration hadoopConfiguration =
        spark.sparkContext().hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouseLocation;
    try {
      warehouseLocation = FileSystem.get(new URI(location), hadoopConfiguration);
    } catch (final IOException e) {
      throw new PersistenceError("Problem accessing location: " + location, e);
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException("Problem parsing URL: " + location, e);
    }
    requireNonNull(warehouseLocation);
    return warehouseLocation;
  }

  /**
   * Rename the partitioned file to follow the naming convention in {@link
   * FileSource#resourceNameWithQualifierMapper(String)}.
   *
   * <p>Each file is named for its partition alone, {@code <name>.<partId>.<extension>}, so a second
   * write to the same location produces the same names. Where a write may be added to an earlier
   * one, use {@link #renamePartitionedFilesKeepingWriteId(SparkSession, String, String, String)}.
   *
   * @param spark the Spark session
   * @param partitionedLocation the location URL containing the partitioned file
   * @param departitionedLocation the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the locations of the resulting (renamed) file
   * @throws PersistenceError if there is a problem renaming a partition file, in which case the
   *     partitioned directory is left in place
   */
  @Nonnull
  public static Collection<String> renamePartitionedFiles(
      @Nonnull final SparkSession spark,
      @Nonnull final String partitionedLocation,
      @Nonnull final String departitionedLocation,
      @Nonnull final String extension) {
    return renamePartitionedFiles(
        getFileSystem(spark, partitionedLocation),
        partitionedLocation,
        departitionedLocation,
        extension);
  }

  /**
   * Rename the partitioned file to follow the pathling naming convention in {@link
   * au.csiro.pathling.library.io.source.FileSource#resourceNameWithQualifierMapper(String)}.
   *
   * <p>Each file is named for its partition alone, {@code <name>.<partId>.<extension>}.
   *
   * @param partitionedLocation a Hadoop {@link FileSystem} representing the location that both the
   *     partitioned and departitioned files are located in
   * @param partitionedUrl the URL of the partitioned file
   * @param departitionedUrl the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the locations of the resulting (renamed) file
   * @throws PersistenceError if there is a problem renaming a partition file, in which case the
   *     partitioned directory is left in place
   */
  @Nonnull
  public static Collection<String> renamePartitionedFiles(
      @Nonnull final FileSystem partitionedLocation,
      @Nonnull final String partitionedUrl,
      @Nonnull final String departitionedUrl,
      @Nonnull final String extension) {
    return departition(
        partitionedLocation,
        partitionedUrl,
        departitionedUrl,
        extension,
        FileSystemPersistence::partitionId);
  }

  /**
   * Rename the partitioned file to follow the naming convention in {@link
   * FileSource#resourceNameWithQualifierMapper(String)}, keeping the id Spark gave the write.
   *
   * <p>Each file is named {@code <name>.<partId>-<writeId>.<extension>}, for example {@code
   * Patient.00000-7a60a041-0e7d-4e24-b23d-82405a665677.parquet}. Spark gives every write an id of
   * its own, so the files of a write never take the names of an earlier write's files in the same
   * location, and a write can be added beside them.
   *
   * @param spark the Spark session
   * @param partitionedLocation the location URL containing the partitioned file
   * @param departitionedLocation the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the locations of the resulting (renamed) files
   * @throws PersistenceError if a partition file is not named as Spark names them, or there is a
   *     problem renaming one, in which case the partitioned directory is left in place
   */
  @Nonnull
  public static Collection<String> renamePartitionedFilesKeepingWriteId(
      @Nonnull final SparkSession spark,
      @Nonnull final String partitionedLocation,
      @Nonnull final String departitionedLocation,
      @Nonnull final String extension) {
    return renamePartitionedFilesKeepingWriteId(
        getFileSystem(spark, partitionedLocation),
        partitionedLocation,
        departitionedLocation,
        extension);
  }

  /**
   * Rename the partitioned file to follow the naming convention in {@link
   * FileSource#resourceNameWithQualifierMapper(String)}, keeping the id Spark gave the write, as
   * {@link #renamePartitionedFilesKeepingWriteId(SparkSession, String, String, String)} describes.
   *
   * @param partitionedLocation a Hadoop {@link FileSystem} representing the location that both the
   *     partitioned and departitioned files are located in
   * @param partitionedUrl the URL of the partitioned file
   * @param departitionedUrl the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the locations of the resulting (renamed) files
   * @throws PersistenceError if a partition file is not named as Spark names them, or there is a
   *     problem renaming one, in which case the partitioned directory is left in place
   */
  @Nonnull
  public static Collection<String> renamePartitionedFilesKeepingWriteId(
      @Nonnull final FileSystem partitionedLocation,
      @Nonnull final String partitionedUrl,
      @Nonnull final String departitionedUrl,
      @Nonnull final String extension) {
    return departition(
        partitionedLocation,
        partitionedUrl,
        departitionedUrl,
        extension,
        FileSystemPersistence::partitionAndWriteId);
  }

  @Nonnull
  private static Collection<String> departition(
      @Nonnull final FileSystem partitionedLocation,
      @Nonnull final String partitionedUrl,
      @Nonnull final String departitionedUrl,
      @Nonnull final String extension,
      @Nonnull final UnaryOperator<String> partitionQualifier) {
    try {
      final Path partitionedPath = new Path(partitionedUrl);
      final FileStatus[] partitionFiles = partitionedLocation.listStatus(partitionedPath);

      final Collection<String> targetFiles =
          Arrays.stream(partitionFiles)
              .map(f -> f.getPath().toString())
              .filter(f -> f.endsWith("." + extension))
              .toList();
      if (targetFiles.isEmpty()) {
        throw new IOException("Partition file not found");
      }
      final String[] departitionFilenameAndExt = new Path(departitionedUrl).getName().split("\\.");
      if (departitionFilenameAndExt.length != 2) {
        throw new PersistenceError(
            ("Unexpected departitioning filename structure. Expected %s to contain exactly one "
                    + "FHIR resource type and the ndjson extension (separated with a '.', i.e. "
                    + "'Patient.ndjson'.")
                .formatted(partitionedPath.getName()),
            null);
      }

      final List<String> renamedFiles = new ArrayList<>();
      for (final String fileName : targetFiles) {
        final String renamedFilename =
            "%s.%s.%s"
                .formatted(
                    departitionFilenameAndExt[0],
                    partitionQualifier.apply(fileName),
                    departitionFilenameAndExt[1]);
        final Path renamedPath = new Path(new Path(departitionedUrl).getParent(), renamedFilename);
        log.info("Renaming result to: {}", renamedPath);
        // A rename that fails, as one onto an existing file may, must not be followed by the
        // removal of the partitioned directory, which still holds the file.
        if (!partitionedLocation.rename(new Path(fileName), renamedPath)) {
          throw new PersistenceError(
              ("Could not rename the partition file %s to %s; the partitioned directory %s has "
                      + "been left in place")
                  .formatted(fileName, renamedPath, partitionedUrl),
              null);
        }
        renamedFiles.add(renamedPath.toString());
      }
      log.info("Cleaning up: {}", partitionedUrl);
      partitionedLocation.delete(partitionedPath, true);
      return renamedFiles;
    } catch (final IOException e) {
      throw new PersistenceError("Problem copying partition file", e);
    }
  }

  /**
   * Returns the partition id of a file Spark wrote, the text after the first '-' in its name, for
   * example {@code 00000} from {@code part-00000-<writeId>-c000.txt}.
   */
  @Nonnull
  private static String partitionId(@Nonnull final String fileName) {
    final String[] partIdSplit =
        FilenameUtils.removeExtension(new Path(fileName).getName()).split("-");
    if (partIdSplit.length < 2) {
      throw new PersistenceError(
          ("Unexpected spark partitioning structure. Expected %s to have the partitioned id "
                  + "after the first '-'")
              .formatted(fileName),
          null);
    }
    return partIdSplit[1];
  }

  /**
   * Returns the partition id and the write id of a file Spark wrote, for example {@code
   * 00000-<writeId>} from {@code part-00000-<writeId>-c000.snappy.parquet}.
   */
  @Nonnull
  private static String partitionAndWriteId(@Nonnull final String fileName) {
    final Matcher matcher = SPARK_PART_FILE.matcher(new Path(fileName).getName());
    if (!matcher.matches()) {
      throw new PersistenceError(
          ("Unexpected spark partitioning structure. Expected %s to be named "
                  + "part-<partitionId>-<writeId>")
              .formatted(fileName),
          null);
    }
    return matcher.group(1) + "-" + matcher.group(2);
  }
}

/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
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

  private FileSystemPersistence() {
  }

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
  public static FileSystem getFileSystem(@Nonnull final SparkSession spark,
      @Nonnull final String location) {
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
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
   * Rename the partitioned file to follow the pathling naming convention in
   * {@link au.csiro.pathling.library.io.source.FileSource#resourceNameWithQualifierMapper(String)}.
   * 
   * @param spark the Spark session
   * @param partitionedLocation the location URL containing the partitioned file
   * @param departitionedLocation the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the locations of the resulting (renamed) file
   */
  @Nonnull
  public static Collection<String> renamePartitionedFiles(@Nonnull final SparkSession spark,
      @Nonnull final String partitionedLocation,
      @Nonnull final String departitionedLocation, @Nonnull final String extension) {
    return renamePartitionedFiles(getFileSystem(spark, partitionedLocation), partitionedLocation,
        departitionedLocation, extension);
  }

  /**
   * Rename the partitioned file to follow the pathling naming convention in
   * {@link au.csiro.pathling.library.io.source.FileSource#resourceNameWithQualifierMapper(String)}.
   *
   * @param partitionedLocation a Hadoop {@link FileSystem} representing the location that both the
   * partitioned and departitioned files are located in
   * @param partitionedUrl the URL of the partitioned file
   * @param departitionedUrl the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the locations of the resulting (renamed) file
   * @throws PersistenceError if there is a problem copying the partition file
   */
  @Nonnull
  public static Collection<String> renamePartitionedFiles(@Nonnull final FileSystem partitionedLocation,
      @Nonnull final String partitionedUrl, @Nonnull final String departitionedUrl,
      @Nonnull final String extension) {
    try {
      final Path partitionedPath = new Path(partitionedUrl);
      final FileStatus[] partitionFiles = partitionedLocation.listStatus(partitionedPath);
      
      final Collection<String> targetFiles = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith("." + extension))
          .toList();
      if(targetFiles.isEmpty()) {
        throw new IOException("Partition file not found");
      }
      String[] departitionFilenameAndExt = new Path(departitionedUrl).getName().split("\\.");
      if(departitionFilenameAndExt.length != 2) {
        throw new PersistenceError("Unexpected departitioning filename structure. Expected %s to contain exactly one FHIR resource type and the ndjson extension (separated with a '.', i.e. 'Patient.ndjson'.".formatted(partitionedPath.getName()), null);
      }

      List<String> renamedFiles = new ArrayList<>();
      for (String fileName : targetFiles) {
        Path filenamePath = new Path(fileName);
        
        String[] partIdSplit = FilenameUtils.removeExtension(filenamePath.getName()).split("-");
        if(partIdSplit.length < 2) {
          throw new PersistenceError("Unexpected spark partitioning structure. Expected %s to have the partitioned id after the first '-'".formatted(fileName), null);
        }
        String renamedFilename = "%s.%s.%s".formatted(departitionFilenameAndExt[0], partIdSplit[1], departitionFilenameAndExt[1]);
        Path renamedPath = new Path(new Path(departitionedUrl).getParent(), renamedFilename);
        log.info("Renaming result to: {}", renamedPath);
        partitionedLocation.rename(new Path(fileName), renamedPath);
        renamedFiles.add(renamedPath.toString());
      }
      log.info("Cleaning up: {}", partitionedUrl);
      partitionedLocation.delete(partitionedPath, true);
      return renamedFiles;
    } catch (final IOException e) {
      throw new PersistenceError("Problem copying partition file", e);
    }
  }

}

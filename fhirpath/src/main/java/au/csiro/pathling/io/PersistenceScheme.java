/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Methods relating to the persistence of data.
 *
 * @author John Grimes
 */
@Slf4j
public abstract class PersistenceScheme {

  /**
   * @param warehouseUrl the URL of the warehouse location
   * @param databaseName the name of the database within the warehouse
   * @param resourceType the resource type to be read or written to
   * @return the URL of the resource within the warehouse
   */
  @Nonnull
  public static String getTableUrl(@Nonnull final String warehouseUrl,
      @Nonnull final String databaseName, @Nonnull final ResourceType resourceType) {
    return String.join("/", warehouseUrl, databaseName, fileNameForResource(resourceType));
  }

  /**
   * @param resourceType A HAPI {@link ResourceType} describing the type of resource
   * @return The filename that should be used
   */
  @Nonnull
  public static String fileNameForResource(@Nonnull final ResourceType resourceType) {
    return resourceType.toCode() + ".parquet";
  }

  /**
   * @param s3Url The S3 URL that should be converted
   * @return A S3A URL
   */
  @Nonnull
  public static String convertS3ToS3aUrl(@Nonnull final String s3Url) {
    return s3Url.replaceFirst("s3:", "s3a:");
  }

  /**
   * @param s3aUrl The S3A URL that should be converted
   * @return A S3 URL
   */
  @Nonnull
  public static String convertS3aToS3Url(@Nonnull final String s3aUrl) {
    return s3aUrl.replaceFirst("s3a:", "s3:");
  }

  /**
   * Convert a directory containing a single file partition into a single file.
   *
   * @param spark the Spark session
   * @param partitionedLocation the location URL containing the partitioned file
   * @param departitionedLocation the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the location of the resulting file
   */
  @Nonnull
  public static String departitionResult(@Nonnull final SparkSession spark,
      @Nonnull final String partitionedLocation,
      @Nonnull final String departitionedLocation, @Nonnull final String extension) {
    return departitionResult(getFileSystem(spark, partitionedLocation), partitionedLocation,
        departitionedLocation, extension);
  }

  /**
   * Convert a directory containing a single file partition into a single file.
   *
   * @param partitionedLocation a Hadoop {@link FileSystem} representing the location that both the
   * partitioned and departitioned files are located in
   * @param partitionedUrl the URL of the partitioned file
   * @param departitionedUrl the desired URL of the resulting file
   * @param extension the file extension used within the partitioned directory
   * @return the location of the resulting file
   */
  @Nonnull
  public static String departitionResult(@Nonnull final FileSystem partitionedLocation,
      @Nonnull final String partitionedUrl, @Nonnull final String departitionedUrl,
      @Nonnull final String extension) {
    try {
      final Path partitionedPath = new Path(partitionedUrl);
      final FileStatus[] partitionFiles = partitionedLocation.listStatus(partitionedPath);
      final String targetFile = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith("." + extension))
          .findFirst()
          .orElseThrow(() -> new IOException("Partition file not found"));
      log.info("Renaming result to: " + departitionedUrl);
      partitionedLocation.rename(new Path(targetFile), new Path(departitionedUrl));
      log.info("Cleaning up: " + partitionedUrl);
      partitionedLocation.delete(partitionedPath, true);
    } catch (final IOException e) {
      throw new RuntimeException("Problem copying partition file", e);
    }
    return departitionedUrl;
  }

  /**
   * Get a Hadoop {@link FileSystem} for the given location.
   *
   * @param spark the Spark session
   * @param location the location URL to be accessed
   * @return the {@link FileSystem} for the given location
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
      throw new RuntimeException("Problem accessing location: " + location, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing URL: " + location, e);
    }
    requireNonNull(warehouseLocation);
    return warehouseLocation;
  }

}

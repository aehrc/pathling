/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.utilities;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

@Slf4j
public abstract class Datasets {

  /**
   * Writes a result to the configured result storage area.
   *
   * @param result the {@link Dataset} containing the result
   * @param fileUrl a name to use as the filename
   * @param saveMode the {@link SaveMode} to use
   * @return the URL of the result
   */
  public static String writeCsv(@Nonnull final Dataset<?> result, @Nonnull final String fileUrl,
      @Nonnull final SaveMode saveMode) {

    Preconditions.check(fileUrl.endsWith(".csv"), "fileUrl must have .csv extension");

    final SparkSession spark = result.sparkSession();

    // Get a handle for the Hadoop FileSystem representing the result location, and check that it
    // is accessible.
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouseLocation;
    try {
      warehouseLocation = FileSystem.get(new URI(fileUrl), hadoopConfiguration);
    } catch (final IOException e) {
      throw new RuntimeException("Problem accessing result location: " + fileUrl, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing result URL: " + fileUrl, e);
    }
    requireNonNull(warehouseLocation);

    // Write result dataset to result location.
    final String resultDatasetUrl = fileUrl + ".tmp";
    log.info("Writing result: " + resultDatasetUrl);
    try {
      result.coalesce(1)
          .write()
          .mode(saveMode)
          .csv(resultDatasetUrl);
    } catch (final Exception e) {
      throw new RuntimeException("Problem writing to file: " + resultDatasetUrl, e);
    }

    // Find the single file and copy it into the final location.
    try {
      final Path resultPath = new Path(resultDatasetUrl);
      final FileStatus[] partitionFiles = warehouseLocation.listStatus(resultPath);
      final String targetFile = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith(".csv"))
          .findFirst()
          .orElseThrow(() -> new IOException("Partition file not found"));
      log.info("Renaming result to: " + fileUrl);
      warehouseLocation.rename(new Path(targetFile), new Path(fileUrl));
      log.info("Cleaning up: " + resultDatasetUrl);
      warehouseLocation.delete(resultPath, true);
    } catch (final IOException e) {
      throw new RuntimeException("Problem copying partition file", e);
    }
    return fileUrl;
  }

}

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

import static au.csiro.pathling.io.FileSystemPersistence.convertS3ToS3aUrl;
import static au.csiro.pathling.io.FileSystemPersistence.departitionResult;
import static au.csiro.pathling.io.FileSystemPersistence.getFileSystem;

import au.csiro.pathling.config.ServerConfiguration;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to persist a Dataset of resources within a specified database.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class ResultWriter {

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final SparkSession spark;

  /**
   * @param configuration a {@link ServerConfiguration} object which controls the behaviour of the
   * writer
   * @param spark the current {@link SparkSession}
   */
  public ResultWriter(@Nonnull final ServerConfiguration configuration,
      @Nonnull final SparkSession spark) {
    this.configuration = configuration;
    this.spark = spark;
  }

  /**
   * Writes a result to the configured result storage area.
   *
   * @param result the {@link Dataset} containing the result
   * @param name a name to use as the filename
   * @return the URL of the result
   */
  public String write(@Nonnull final Dataset<?> result, @Nonnull final String name) {
    return write(result, name, SaveMode.ErrorIfExists);
  }

  /**
   * Writes a result to the configured result storage area.
   *
   * @param result the {@link Dataset} containing the result
   * @param name a name to use as the filename
   * @param saveMode the {@link SaveMode} to use
   * @return the URL of the result
   */
  public String write(@Nonnull final Dataset<?> result, @Nonnull final String name,
      @Nonnull final SaveMode saveMode) {
    final String warehouseUrl = convertS3ToS3aUrl(configuration.getStorage().getWarehouseUrl());

    // Get a handle for the Hadoop FileSystem representing the result location, and check that it
    // is accessible.
    final FileSystem warehouseLocation = getFileSystem(spark, warehouseUrl);

    // Write result dataset to result location.
    final String resultFileUrl = warehouseUrl + "/results/" + name;
    log.info("Writing result: " + resultFileUrl);
    try {
      result.coalesce(1)
          .write()
          .mode(saveMode)
          .csv(resultFileUrl);
    } catch (final Exception e) {
      throw new RuntimeException("Problem writing to file: " + resultFileUrl, e);
    }

    // Find the single file and copy it into the final location.
    return departitionResult(warehouseLocation, resultFileUrl, resultFileUrl + ".csv", "csv");
  }

}

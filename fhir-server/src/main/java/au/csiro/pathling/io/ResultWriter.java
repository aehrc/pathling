/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.Configuration;
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
  private final Configuration configuration;

  @Nonnull
  private final SparkSession spark;

  /**
   * @param configuration a {@link Configuration} object which controls the behaviour of the writer
   * @param spark the current {@link SparkSession}
   */
  public ResultWriter(@Nonnull final Configuration configuration,
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
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    checkNotNull(hadoopConfiguration);
    @Nullable final FileSystem warehouseLocation;
    try {
      warehouseLocation = FileSystem.get(new URI(warehouseUrl), hadoopConfiguration);
    } catch (final IOException e) {
      throw new RuntimeException("Problem accessing result location: " + warehouseUrl, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing result URL: " + warehouseUrl, e);
    }
    checkNotNull(warehouseLocation);

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
    final String targetUrl = resultFileUrl + ".csv";
    try {
      final Path resultPath = new Path(resultFileUrl);
      final FileStatus[] partitionFiles = warehouseLocation.listStatus(resultPath);
      final String targetFile = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith(".csv"))
          .findFirst()
          .orElseThrow(() -> new IOException("Partition file not found"));
      log.info("Renaming result to: " + targetUrl);
      warehouseLocation.rename(new Path(targetFile), new Path(targetUrl));
      log.info("Cleaning up: " + resultFileUrl);
      warehouseLocation.delete(resultPath, true);
    } catch (final IOException e) {
      throw new RuntimeException("Problem copying partition file", e);
    }

    return targetUrl;
  }

}

/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Facilitates the reading of the content of extract result files.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class ResultReader {

  @Nonnull
  private final SparkSession spark;

  /**
   * @param spark the current {@link SparkSession}
   */
  public ResultReader(@Nonnull final SparkSession spark) {
    this.spark = spark;
  }

  /**
   * @param resultUrl the URL of a result file
   * @return an {@link InputStream} containing the contents of the file
   */
  public InputStream read(@Nonnull final String resultUrl) {
    // Initialise a Hadoop FileSystem instance centred on the result URL.
    @Nullable final Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
    checkNotNull(hadoopConfiguration);
    @Nullable final FileSystem resultLocation;
    final URI resultUri;
    try {
      resultUri = new URI(resultUrl);
      resultLocation = FileSystem.get(resultUri, hadoopConfiguration);
    } catch (final IOException e) {
      throw new RuntimeException("Problem accessing result: " + resultUrl, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing result URL: " + resultUrl, e);
    }
    checkNotNull(resultLocation);

    // Open up an input stream from the result.
    final Path path = new Path(resultUri);
    final FSDataInputStream inputStream;
    try {
      inputStream = resultLocation.open(path);
      log.info("Opened stream from: {}", resultUrl);
    } catch (final IOException e) {
      throw new RuntimeException("Problem reading result: " + resultUrl, e);
    }
    return inputStream;
  }

}

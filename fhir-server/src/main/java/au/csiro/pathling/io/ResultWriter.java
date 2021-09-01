/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.convertS3aToS3Url;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.Configuration;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
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
   * @param configuration A {@link Configuration} object which controls the behaviour of the writer
   * @param spark The current {@link SparkSession}
   */
  public ResultWriter(@Nonnull final Configuration configuration,
      @Nonnull final SparkSession spark) {
    this.configuration = configuration;
    this.spark = spark;
  }

  /**
   * Writes a result to the configured result storage area.
   *
   * @param result The {@link Dataset} containing the result.
   * @return the URL of the result
   */
  public String write(@Nonnull final Dataset result) {
    final String resultUrl = convertS3ToS3aUrl(configuration.getStorage().getResultUrl());

    // Get a handle for the Hadoop FileSystem representing the result location, and check that it
    // is accessible.
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    checkNotNull(hadoopConfiguration);
    @Nullable final FileSystem resultLocation;
    try {
      resultLocation = FileSystem.get(new URI(resultUrl), hadoopConfiguration);
    } catch (final IOException e) {
      throw new RuntimeException("Problem accessing result location: " + resultUrl, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing result URL: " + resultUrl, e);
    }
    checkNotNull(resultLocation);

    // Write result dataset to result location.
    final String resultFileUrl = resultUrl + "/" + UUID.randomUUID();
    log.info("Writing result: " + resultFileUrl);
    try {
      result.coalesce(1)
          .write()
          .mode(SaveMode.ErrorIfExists)
          .csv(resultFileUrl);
    } catch (final Exception e) {
      throw new RuntimeException("Problem writing to file: " + resultFileUrl, e);
    }

    // Find the single file and copy it into the final location.
    final String targetUrl = resultFileUrl + ".csv";
    try {
      final Path resultPath = new Path(resultFileUrl);
      final FileStatus[] partitionFiles = resultLocation.listStatus(resultPath);
      final String targetFile = Arrays.stream(partitionFiles)
          .map(f -> f.getPath().toString())
          .filter(f -> f.endsWith(".csv"))
          .findFirst()
          .orElseThrow(() -> new IOException("Partition file not found"));
      log.info("Renaming result to: " + targetUrl);
      resultLocation.rename(new Path(targetFile), new Path(targetUrl));
      log.info("Cleaning up: " + resultFileUrl);
      resultLocation.delete(resultPath, true);
    } catch (final IOException e) {
      throw new RuntimeException("Problem copying partition file", e);
    }

    // If the result is an S3 URL, it will need to be converted into a signed URL.
    final String convertedUrl = convertS3aToS3Url(targetUrl);
    if (convertedUrl.startsWith("s3://")) {
      final URI parsedUrl;
      try {
        parsedUrl = new URI(convertedUrl);
      } catch (final URISyntaxException e) {
        throw new RuntimeException("Problem parsing S3 result url: " + convertedUrl, e);
      }
      return generateSignedS3Url(configuration.getStorage().getAws(),
          parsedUrl.getHost(), parsedUrl.getPath().replaceFirst("/", ""));
    } else {
      return convertedUrl;
    }
  }

  @Nonnull
  private static String generateSignedS3Url(@Nonnull final Configuration.Storage.Aws awsConfig,
      @Nonnull final String bucketName, @Nonnull final String objectKey) {
    final String accessKeyId = checkPresent(awsConfig.getAccessKeyId());
    final String secretAccessKey = checkPresent(awsConfig.getSecretAccessKey());
    final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new BasicAWSCredentialsProvider(accessKeyId, secretAccessKey))
        .build();
    final Date expiration = new Date();
    final long expiryTimeMilliseconds =
        Instant.now().toEpochMilli() + (awsConfig.getSignedUrlExpiry() * 1000);
    expiration.setTime(expiryTimeMilliseconds);
    final GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName,
        objectKey)
        .withMethod(HttpMethod.GET)
        .withExpiration(expiration);
    log.info("Generating signed URL: {}, {}, {}, {}", request.getMethod(), request.getExpiration(),
        request.getBucketName(), request.getKey());
    return s3Client.generatePresignedUrl(request).toExternalForm();
  }

}

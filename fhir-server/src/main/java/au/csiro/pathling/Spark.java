/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Provides an Apache Spark session for use by the Pathling server.
 *
 * @author John Grimes
 */
@Component
@Slf4j
public class Spark {

  /**
   * @param configuration A {@link Configuration} object containing the parameters to use in the
   * creation
   * @return A shiny new {@link SparkSession}
   */
  @Bean
  @Autowired
  @Nonnull
  public static SparkSession build(@Nonnull final Configuration configuration) {
    log.info("Creating Spark session");
    final SparkSession spark = SparkSession.builder()
        .appName("pathling-server")
        .config("spark.master", configuration.getSparkMasterUrl())
        .config("spark.executor.memory", configuration.getExecutorMemory())
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", configuration.getShufflePartitions())
        .getOrCreate();
    if (configuration.getAwsAccessKeyId().isPresent()
        && configuration.getAwsSecretAccessKey().isPresent()) {
      final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
          .hadoopConfiguration();
      hadoopConfiguration.set("fs.s3a.access.key", configuration.getAwsAccessKeyId().get());
      hadoopConfiguration.set("fs.s3a.secret.key", configuration.getAwsSecretAccessKey().get());
    }
    return spark;
  }

}

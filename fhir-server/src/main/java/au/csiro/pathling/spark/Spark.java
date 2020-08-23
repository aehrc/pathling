/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.spark;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Storage.Aws;
import au.csiro.pathling.spark.udf.CodingsEqual;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.*;
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
   * @param environment Spring {@link Environment} from which to harvest Spark configuration
   * @return A shiny new {@link SparkSession}
   */
  @Bean
  @Autowired
  @Nonnull
  public static SparkSession build(@Nonnull final Configuration configuration,
      @Nonnull final Environment environment) {
    log.info("Creating Spark session");
    resolveSparkConfiguration(environment);

    final SparkSession spark = SparkSession.builder()
        .appName(configuration.getSpark().getAppName())
        // The Spark 3 analyzer throws an error by default when joining a table with itself using
        // columns with the same name. Because we ensure that the columns are in fact the same
        // thing, and it doesn't matter which to choose, ignoring these errors is fine within this
        // implementation. This saves us doing a whole bunch of fancy aliasing for no benefit.
        .config("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
        .getOrCreate();

    // Configure user defined functions.
    spark.udf().register("codings_equal", new CodingsEqual(), DataTypes.BooleanType);

    // Configure AWS driver and credentials.
    final Aws awsConfig = configuration.getStorage().getAws();
    final org.apache.hadoop.conf.Configuration hadoopConfig = spark.sparkContext()
        .hadoopConfiguration();
    if (awsConfig.isAnonymousAccess()) {
      hadoopConfig.set("fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    }
    awsConfig.getAccessKeyId()
        .ifPresent(accessKeyId -> hadoopConfig.set("fs.s3a.access.key", accessKeyId));
    awsConfig.getSecretAccessKey()
        .ifPresent(secretAccessKey -> hadoopConfig.set("fs.s3a.secret.key", secretAccessKey));

    return spark;
  }

  private static void resolveSparkConfiguration(@Nonnull final PropertyResolver resolver) {
    // This goes through the properties within the Spring configuration and copies the Spark
    // configuration into Java system properties, which Spark will then pick up.
    final MutablePropertySources propertySources = ((AbstractEnvironment) resolver)
        .getPropertySources();
    propertySources.stream()
        .filter(propertySource -> propertySource instanceof EnumerablePropertySource)
        .flatMap(propertySource -> Arrays
            .stream(((EnumerablePropertySource) propertySource).getPropertyNames()))
        .filter(property -> property.startsWith("spark."))
        .forEach(property -> System.setProperty(property,
            Objects.requireNonNull(resolver.getProperty(property))));
  }

}

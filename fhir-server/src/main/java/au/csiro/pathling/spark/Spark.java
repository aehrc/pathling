/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.spark;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.config.StorageConfiguration.Aws;
import au.csiro.pathling.async.SparkListener;
import au.csiro.pathling.sql.SqlStrategy;
import au.csiro.pathling.sql.udf.SqlFunction1;
import au.csiro.pathling.sql.udf.SqlFunction2;
import au.csiro.pathling.sql.udf.SqlFunction3;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertyResolver;
import org.springframework.stereotype.Component;

/**
 * Provides an Apache Spark session for use by the Pathling server.
 *
 * @author John Grimes
 */
@Component
@Profile({"core", "spark"})
@Slf4j
public class Spark {

  /**
   * @param configuration a {@link Configuration} object containing the parameters to use in the
   * creation
   * @param environment Spring {@link Environment} from which to harvest Spark configuration
   * @param sparkListener a {@link SparkListener} that is used to monitor progress of jobs
   * @param sqlFunction1 a list of {@link SqlFunction1} that should be registered
   * @param sqlFunction2 a list of {@link SqlFunction2} that should be registered
   * @param sqlFunction3 a list of {@link SqlFunction3} that should be registered
   * @return A shiny new {@link SparkSession}
   */
  @Bean(destroyMethod = "stop")
  @ConditionalOnMissingBean
  @Nonnull
  public static SparkSession build(@Nonnull final Configuration configuration,
      @Nonnull final Environment environment,
      @Nonnull final Optional<SparkListener> sparkListener,
      @Nonnull final List<SqlFunction1<?, ?>> sqlFunction1,
      @Nonnull final List<SqlFunction2<?, ?, ?>> sqlFunction2,
      @Nonnull final List<SqlFunction3<?, ?, ?, ?>> sqlFunction3) {
    log.debug("Creating Spark session");
    resolveSparkConfiguration(environment);

    final SparkSession spark = SparkSession.builder()
        .appName(configuration.getSpark().getAppName())
        .getOrCreate();
    sparkListener.ifPresent(l -> spark.sparkContext().addSparkListener(l));

    // Configure user defined strategy and functions.
    SqlStrategy.setup(spark);
    for (final SqlFunction1<?, ?> function : sqlFunction1) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction2<?, ?, ?> function : sqlFunction2) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction3<?, ?, ?, ?> function : sqlFunction3) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }

    // Configure AWS driver and credentials.
    configureAwsDriver(configuration, spark);

    return spark;
  }

  private static void configureAwsDriver(@Nonnull final Configuration configuration,
      @Nonnull final SparkSession spark) {
    final Aws awsConfig = configuration.getStorage().getAws();
    final org.apache.hadoop.conf.Configuration hadoopConfig = spark.sparkContext()
        .hadoopConfiguration();

    // We need to use the anonymous credentials provider if we are not using AWS credentials.
    if (awsConfig.isAnonymousAccess()) {
      hadoopConfig.set("fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    }

    // Set credentials if provided.
    awsConfig.getAccessKeyId()
        .ifPresent(accessKeyId -> hadoopConfig.set("fs.s3a.access.key", accessKeyId));
    awsConfig.getSecretAccessKey()
        .ifPresent(secretAccessKey -> hadoopConfig.set("fs.s3a.secret.key", secretAccessKey));
    hadoopConfig.set("fs.s3a.connection.maximum", "100");
    hadoopConfig.set("fs.s3a.committer.magic.enabled", "true");
    hadoopConfig.set("fs.s3a.committer.name", "magic");

    // Assume role if configured.
    awsConfig.getAssumedRole()
        .ifPresent(assumedRole -> {
          hadoopConfig.set("fs.s3a.aws.credentials.provider",
              "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
          hadoopConfig.set("fs.s3a.assumed.role.arn", assumedRole);
        });
  }

  private static void resolveSparkConfiguration(@Nonnull final PropertyResolver resolver) {
    // This goes through the properties within the Spring configuration and copies the Spark
    // configuration into Java system properties, which Spark will then pick up.
    final MutablePropertySources propertySources = ((AbstractEnvironment) resolver)
        .getPropertySources();
    propertySources.stream()
        .filter(propertySource -> propertySource instanceof EnumerablePropertySource)
        .flatMap(propertySource -> Arrays
            .stream(((EnumerablePropertySource<?>) propertySource).getPropertyNames()))
        .filter(property -> property.startsWith("spark."))
        .forEach(property -> System.setProperty(property,
            Objects.requireNonNull(resolver.getProperty(property))));
  }

}

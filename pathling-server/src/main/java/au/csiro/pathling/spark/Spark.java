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

package au.csiro.pathling.spark;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.async.SparkJobListener;
import au.csiro.pathling.config.ServerConfiguration;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
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
   * @param configuration a {@link ServerConfiguration} object containing the parameters to use in
   * the creation
   * @param environment Spring {@link Environment} from which to harvest Spark configuration
   * @param sparkListener a {@link SparkJobListener} that is used to monitor progress of jobs
   * @param sparkConfigurers a list of {@link SparkConfigurer} that should use to configure spark
   * session
   * @return A shiny new {@link SparkSession}
   */
  @Bean(destroyMethod = "stop")
  @ConditionalOnMissingBean
  @Nonnull
  public static SparkSession build(@Nonnull final ServerConfiguration configuration,
      @Nonnull final Environment environment,
      @Nonnull final Optional<SparkJobListener> sparkListener,
      @Nonnull final List<SparkConfigurer> sparkConfigurers) {
    log.debug("Creating Spark session");

    // Pass through Spark configuration.
    resolveThirdPartyConfiguration(environment, List.of("spark."),
        property -> System.setProperty(property,
            requireNonNull(environment.getProperty(property))));

    final SparkSession spark = SparkSession.builder()
            .appName(configuration.getSpark().getAppName())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    sparkListener.ifPresent(l -> spark.sparkContext().addSparkListener(l));

    // Configure user defined strategy and functions.
    for (final SparkConfigurer configurer : sparkConfigurers) {
      configurer.configure(spark);
    }

    // Pass through Hadoop AWS configuration.
    resolveThirdPartyConfiguration(environment, List.of("fs.s3a."),
        property -> spark.sparkContext().hadoopConfiguration().set(property,
            requireNonNull(environment.getProperty(property))));

    return spark;
  }

  private static void resolveThirdPartyConfiguration(@Nonnull final PropertyResolver resolver,
      @Nonnull final List<String> prefixes, @Nonnull final Consumer<String> setter) {
    // This goes through the properties within the Spring configuration and invokes the provided 
    // setter function for each property that matches one of the supplied prefixes.
    final MutablePropertySources propertySources = ((AbstractEnvironment) resolver)
        .getPropertySources();
    propertySources.stream()
        .filter(propertySource -> propertySource instanceof EnumerablePropertySource)
        .flatMap(propertySource -> Arrays
            .stream(((EnumerablePropertySource<?>) propertySource).getPropertyNames()))
        .filter(property -> prefixes.stream().anyMatch(property::startsWith))
        .forEach(setter);
  }

}

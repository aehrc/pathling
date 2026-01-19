/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Provides beans used within processing of FHIR requests within the server.
 *
 * @author John Grimes
 */
@Component
@Slf4j
public class Dependencies {

  private Dependencies() {}

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static PathlingServerVersion serverVersion() {
    return new PathlingServerVersion();
  }

  @Bean
  @ConditionalOnMissingBean
  static PathlingContext pathlingContext(
      @Nonnull final SparkSession spark, @Nonnull final ServerConfiguration config) {
    return PathlingContext.builder(spark)
        .encodingConfiguration(config.getEncoding())
        .terminologyConfiguration(config.getTerminology())
        .build();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirContext fhirContext(@Nonnull final PathlingContext pathlingContext) {
    log.debug("Creating R4 FHIR context");
    return pathlingContext.getFhirContext();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static IParser jsonParser(@Nonnull final FhirContext fhirContext) {
    return fhirContext.newJsonParser();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static QueryableDataSource deltaLake(
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final ServerConfiguration serverConfiguration) {
    final String databaseLocation =
        serverConfiguration.getStorage().getWarehouseUrl()
            + "/"
            + serverConfiguration.getStorage().getDatabaseName();
    final QueryableDataSource baseSource = pathlingContext.read().delta(databaseLocation);
    return new DynamicDeltaSource(
        baseSource,
        pathlingContext.getSpark(),
        databaseLocation,
        pathlingContext.getFhirEncoders());
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirEncoders fhirEncoders(@Nonnull final PathlingContext pathlingContext) {
    return pathlingContext.getFhirEncoders();
  }
}

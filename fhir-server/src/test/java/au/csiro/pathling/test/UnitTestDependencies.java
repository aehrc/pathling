/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.spark.Spark;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.stubs.TestTerminologyClientFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("unit-test")
public class UnitTestDependencies {

  @Bean
  @Nonnull
  public static SparkSession sparkSession(@Nonnull final Configuration configuration,
      @Nonnull final Environment environment) {
    return Spark.build(configuration, environment);
  }

  @Bean
  @Nonnull
  public static FhirContext fhirContext() {
    return FhirContext.forR4();
  }

  @Bean
  @Nonnull
  public static IParser jsonParser(@Nonnull final FhirContext fhirContext) {
    return fhirContext.newJsonParser();
  }

  @Bean
  @Nonnull
  public static FhirEncoders fhirEncoders() {
    return FhirEncoders.forR4().getOrCreate();
  }

  @Bean
  @Nonnull
  public static TerminologyClient terminologyClient() {
    return SharedMocks.getOrCreate(TerminologyClient.class);
  }

  @Bean
  @Nonnull
  public static TerminologyService terminologyService() {
    return SharedMocks.getOrCreate(TerminologyService.class);
  }

  @Bean
  @Nonnull
  public static TerminologyClientFactory terminologyClientFactory() {
    return new TestTerminologyClientFactory();
  }

}

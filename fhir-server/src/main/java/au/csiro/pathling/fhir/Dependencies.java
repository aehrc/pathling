/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Terminology;
import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Provides beans used within processing of FHIR requests within the server.
 *
 * @author John Grimes
 */
@Component
@Profile({"core", "fhir"})
@Slf4j
public class Dependencies {

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirContext fhirContext() {
    log.info("Creating R4 FHIR context");
    return FhirContext.forR4();
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
  static FhirEncoders fhirEncoders(@Nonnull final Configuration configuration) {
    final int maxNestingLevel = configuration.getEncoding().getMaxNestingLevel();
    log.info("Creating R4 FHIR encoders with maxNestingLevel: " + maxNestingLevel);
    return FhirEncoders.forR4().withMaxNestingLevel(maxNestingLevel).getOrCreate();
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(prefix = "pathling", value = "terminology.enabled", havingValue = "true")
  @Nonnull
  static TerminologyClient terminologyClient(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext) {
    final Terminology terminology = configuration.getTerminology();
    checkNotNull(terminology);
    log.info("Creating FHIR terminology client: {}", terminology.getServerUrl());
    return TerminologyClient.build(fhirContext,
        terminology.getServerUrl(),
        terminology.getSocketTimeout(),
        configuration.getVerboseRequestLogging(),
        log);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(TerminologyClient.class)
  @Nonnull
  static TerminologyServiceFactory terminologyClientFactory(
      @Nonnull final Configuration configuration, @Nonnull final FhirContext fhirContext) {
    final Terminology terminology = configuration.getTerminology();
    checkNotNull(terminology);
    return new DefaultTerminologyServiceFactory(fhirContext,
        terminology.getServerUrl(),
        terminology.getSocketTimeout(),
        configuration.getVerboseRequestLogging());
  }

}

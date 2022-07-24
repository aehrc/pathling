/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.config.TerminologyConfiguration;
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
  static PathlingVersion version() {
    return new PathlingVersion();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static FhirContext fhirContext() {
    log.debug("Creating R4 FHIR context");
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
    final boolean enableExtensions = configuration.getEncoding().isEnableExtensions();
    log.debug("Creating R4 FHIR encoders (max nesting level of: {}, and extensions enabled: {})",
        maxNestingLevel, enableExtensions);
    return FhirEncoders.forR4()
        .withMaxNestingLevel(maxNestingLevel)
        .withOpenTypes(configuration.getEncoding().getOpenTypes())
        .withExtensionsEnabled(enableExtensions)
        .getOrCreate();
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(prefix = "pathling", value = "terminology.enabled", havingValue = "true")
  @Nonnull
  static TerminologyClient terminologyClient(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext) {
    final TerminologyConfiguration terminology = configuration.getTerminology();
    log.debug("Creating FHIR terminology client: {}", terminology.getServerUrl());
    return TerminologyClient.build(fhirContext, terminology.getServerUrl(),
        terminology.getSocketTimeout(), terminology.isVerboseLogging(),
        terminology.getAuthentication(), log);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(TerminologyClient.class)
  @Nonnull
  static TerminologyServiceFactory terminologyClientFactory(
      @Nonnull final Configuration configuration, @Nonnull final FhirContext fhirContext) {
    final TerminologyConfiguration terminology = configuration.getTerminology();
    return new DefaultTerminologyServiceFactory(fhirContext, terminology.getServerUrl(),
        terminology.getSocketTimeout(), terminology.isVerboseLogging(),
        terminology.getAuthentication());
  }

}

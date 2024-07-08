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

package au.csiro.pathling.fhir;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
  static StorageConfiguration storageConfiguration(
      @Nonnull final ServerConfiguration configuration) {
    return configuration.getStorage();
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static QueryConfiguration queryConfigurationConfiguration(
      @Nonnull final ServerConfiguration configuration) {
    return configuration.getQuery();
  }

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
  static FhirEncoders fhirEncoders(@Nonnull final ServerConfiguration configuration) {
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
  @Nonnull
  static TerminologyServiceFactory terminologyClientFactory(
      @Nonnull final ServerConfiguration configuration, @Nonnull final FhirContext fhirContext) {
    final TerminologyConfiguration terminology = configuration.getTerminology();
    return new DefaultTerminologyServiceFactory(fhirContext.getVersion().getVersion(), terminology);
  }
}

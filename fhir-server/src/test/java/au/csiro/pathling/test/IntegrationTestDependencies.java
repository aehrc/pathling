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

package au.csiro.pathling.test;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("integration-test")
@Slf4j
public class IntegrationTestDependencies {

  public static final int WIREMOCK_PORT = 4072;

  @Bean(destroyMethod = "stop")
  public WireMockServer wireMockServer() {
    final WireMockServer wireMockServer = new WireMockServer(
        new WireMockConfiguration().port(WIREMOCK_PORT)
            .usingFilesUnderDirectory("src/test/resources/wiremock"));
    WireMock.configureFor("localhost", WIREMOCK_PORT);
    return wireMockServer;
  }

  @Bean
  public DefaultTerminologyServiceFactory terminologyServiceFactory(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration configuration) {
    log.info("Configuration at creation of TerminologyServiceFactory: {}", configuration);
    return new DefaultTerminologyServiceFactory(fhirContext.getVersion().getVersion(),
        configuration.getTerminology());
  }
}

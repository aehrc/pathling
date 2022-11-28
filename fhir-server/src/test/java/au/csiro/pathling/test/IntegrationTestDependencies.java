/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.UUIDFactory;
import ca.uhn.fhir.context.FhirContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.UUID;
import javax.annotation.Nonnull;
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
  public UUIDFactory uuidFactory() {
    return UUID::randomUUID;
  }

  @Bean
  public DefaultTerminologyServiceFactory terminologyServiceFactory(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Configuration configuration, 
      @Nonnull final UUIDFactory uuidFactory) {
    log.info("Configuration at creation of TerminologyServiceFactory: {}", configuration);
    final DefaultTerminologyServiceFactory factory = new DefaultTerminologyServiceFactory(
        fhirContext.getVersion().getVersion(),
        configuration.getTerminology().getServerUrl(), 0, false,
        configuration.getTerminology().getClient(),
        configuration.getTerminology().getCache(),
        configuration.getTerminology().getAuthentication());
    factory.setUUIDFactory(uuidFactory);
    return factory;
  }
  
  @Bean
  public TerminologyService terminologyService(
      @Nonnull final DefaultTerminologyServiceFactory terminologyServiceFactory) throws NoSuchFieldException, IllegalAccessException {
    return terminologyServiceFactory.buildService();
  }

}

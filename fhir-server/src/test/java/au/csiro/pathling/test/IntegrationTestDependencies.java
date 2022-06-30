/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.fhir.DefaultTerminologyServiceFactory;
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

  private static final int WIREMOCK_PORT = 4072;

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
      @Nonnull final Configuration configuration) {
    return new DefaultTerminologyServiceFactory(fhirContext,
        configuration.getTerminology().getServerUrl(), 0, false,
        configuration.getTerminology().getAuthentication());
  }

  @Bean
  public UUIDFactory uuidFactory() {
    return UUID::randomUUID;
  }

  @Bean
  public TerminologyService terminologyService(
      @Nonnull final DefaultTerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final UUIDFactory uuidFactory) {
    return terminologyServiceFactory.buildService(log, uuidFactory);
  }

}

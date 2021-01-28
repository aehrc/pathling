/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("integration-test")
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

}

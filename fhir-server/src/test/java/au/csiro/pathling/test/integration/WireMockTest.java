/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import au.csiro.pathling.test.IntegrationTestDependencies;
import com.github.tomakehurst.wiremock.WireMockServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@TestPropertySource(properties = {
    "pathling.test.recording.terminologyServerUrl=https://tx.ontoserver.csiro.au",
    "pathling.sentryDsn=http://123abc@localhost:" + IntegrationTestDependencies.WIREMOCK_PORT
        + "/5513555",
    "pathling.sentryEnvironment=integration-test"
})
@Slf4j
class WireMockTest extends IntegrationTest {

  @Autowired
  WireMockServer wireMockServer;

  static boolean isRecordMode() {
    return Boolean.parseBoolean(System.getProperty("pathling.test.recording.enabled", "false"));
  }

  @BeforeEach
  void setUp() {
    log.info("Starting WireMock server");
    wireMockServer.start();
  }

  @AfterEach
  void tearDown() {
    log.info("Stopping WireMock server");
    wireMockServer.stop();
  }

}

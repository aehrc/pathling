/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@Slf4j
public class WireMockTest extends IntegrationTest {

  @Autowired
  protected WireMockServer wireMockServer;

  protected static boolean isRecordMode() {
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

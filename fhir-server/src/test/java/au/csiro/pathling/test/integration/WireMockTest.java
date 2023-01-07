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
    "pathling.test.recording.terminologyServerUrl=https://r4.ontoserver.csiro.au",
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

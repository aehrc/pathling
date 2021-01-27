/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;

import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(locations = {"classpath:/configuration/integration-test.properties"})
class CapabilityStatementTest {

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @BeforeAll
  static void beforeAll() {
    // See: https://github.com/spring-projects/spring-boot/issues/21535#issuecomment-634088332
    TomcatURLStreamHandlerFactory.disable();
  }

  @Test
  void capabilityStatement() throws JSONException {
    final String response = restTemplate
        .getForObject("http://localhost:" + port + "/fhir/metadata",
            String.class);
    assertJson("responses/CapabilityStatementTest/capabilityStatement.CapabilityStatement.json",
        response, JSONCompareMode.LENIENT);
  }

}
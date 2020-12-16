/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.TestHelpers;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(locations = {"classpath:/configuration/integration-test.properties"})
public class SearchTest {

  @Autowired
  SparkSession spark;
  @MockBean
  ResourceReader resourceReader;
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
  void searchWithNoFilter() throws URISyntaxException {
    TestHelpers.mockResourceReader(resourceReader, spark, ResourceType.PATIENT);
    final String uri = "http://localhost:" + port + "/fhir/Patient?_summary=false";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    assertTrue(response.getStatusCode().is2xxSuccessful());
  }

}

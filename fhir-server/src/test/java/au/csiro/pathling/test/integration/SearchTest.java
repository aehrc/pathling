/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.TestHelpers;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

/**
 * @author John Grimes
 */
public class SearchTest extends IntegrationTest {

  @Autowired
  SparkSession spark;

  @MockBean
  ResourceReader resourceReader;

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void searchWithNoFilter() throws URISyntaxException {
    TestHelpers.mockResourceReader(resourceReader, spark, ResourceType.PATIENT);
    final String uri = "http://localhost:" + port + "/fhir/Patient?_summary=false";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    assertTrue(response.getStatusCode().is2xxSuccessful());
  }

}

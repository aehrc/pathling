/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsString;
import static au.csiro.pathling.test.helpers.TestHelpers.mockResourceReader;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.ResourceReader;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
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
public class MultipleAggregationsAndResolveTest extends IntegrationTest {

  @Autowired
  SparkSession spark;

  @MockBean
  ResourceReader resourceReader;

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void multipleAggregationsAndResolve() throws URISyntaxException {
    mockResourceReader(resourceReader, spark, 2, ResourceType.PATIENT, ResourceType.CONDITION);
    final String request = getResourceAsString(
        "requests/MultipleAggregationsAndResolveTest/multipleAggregationsAndResolve.Parameters.json");
    final String uri = "http://localhost:" + port + "/fhir/Patient/$aggregate";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.POST,
            RequestEntity.post(new URI(uri)).header("Content-Type", "application/fhir+json")
                .body(request), String.class);
    assertNotNull(response.getBody());
    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertJson(
        "responses/MultipleAggregationsAndResolve/multipleAggregationsAndResolve.Parameters.json",
        response.getBody(), JSONCompareMode.LENIENT);
  }

}

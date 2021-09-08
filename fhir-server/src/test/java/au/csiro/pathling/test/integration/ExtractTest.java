/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import au.csiro.pathling.io.ResourceReader;
import java.net.URISyntaxException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;

/**
 * @author John Grimes
 */
public class ExtractTest extends IntegrationTest {

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
    // TestHelpers.mockResourceReader(resourceReader, spark, ResourceType.DIAGNOSTICREPORT,
    //     ResourceType.OBSERVATION);
    // final String uri = "http://localhost:" + port + "/fhir/DiagnosticReport"
    //     + "?column=" + URLEncoder.encode("id", StandardCharsets.UTF_8)
    //     + "&column=" + URLEncoder.encode("");
    // final ResponseEntity<String> response = restTemplate
    //     .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    // assertTrue(response.getStatusCode().is2xxSuccessful());
    // final List<String> eTags = response.getHeaders().get("ETag");
    // assertNotNull(eTags);
    // assertEquals(1, eTags.size());
    // final String eTag = eTags.get(0);
    // final ResponseEntity<String> response2 = restTemplate
    //     .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri))
    //         .header("If-None-Match", new String[]{eTag})
    //         .build(), String.class);
    // assertEquals(304, response2.getStatusCode().value());
  }

}

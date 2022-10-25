/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.TestResources.assertJson;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

/**
 * @author John Grimes
 */
@Tag("Tranche2")
class OperationDefinitionTest extends IntegrationTest {

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  private static final List<String> OPERATIONS = List.of("aggregate", "search", "extract", "import",
      "result", "job");
  private static final String SUFFIX = "6";

  @Test
  void operationDefinitions() throws MalformedURLException, URISyntaxException {
    for (final String operation : OPERATIONS) {
      final URL url = new URL(
          "http://localhost:" + port + "/fhir/OperationDefinition/" + operation + "-" + SUFFIX);
      final RequestEntity<Void> request = RequestEntity.get(url.toURI()).build();
      final ResponseEntity<String> response =
          restTemplate.exchange(url.toURI(), HttpMethod.GET, request, String.class);
      assertTrue(response.getStatusCode().is2xxSuccessful());

      final String body = response.getBody();
      assertNotNull(body);
      assertJson("fhir/" + operation + ".OperationDefinition.json", body,
          JSONCompareMode.LENIENT);
    }
  }

}

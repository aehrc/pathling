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
  private static final String SUFFIX = "8";

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

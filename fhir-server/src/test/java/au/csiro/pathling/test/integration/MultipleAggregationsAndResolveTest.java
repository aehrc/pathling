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
import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.test.helpers.TestHelpers;
import jakarta.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

/**
 * @author John Grimes
 */
@Tag("Tranche2")
class MultipleAggregationsAndResolveTest extends IntegrationTest {

  @Autowired
  SparkSession spark;

  @MockBean
  CacheableDatabase database;

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @Test
  void multipleAggregationsAndResolve() throws URISyntaxException {
    TestHelpers.mockResource(database, spark, 2, ResourceType.PATIENT, ResourceType.CONDITION);
    final ResponseEntity<String> response = getResponse(
        "requests/MultipleAggregationsAndResolveTest/multipleAggregationsAndResolve.Parameters.json");
    assertNotNull(response.getBody());
    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertJson(
        "responses/MultipleAggregationsAndResolve/multipleAggregationsAndResolve.Parameters.json",
        response.getBody(), JSONCompareMode.LENIENT);
  }

  @Test
  void multipleAggregationsAndResolveWithGroupings() throws URISyntaxException {
    TestHelpers.mockResource(database, spark, 2, ResourceType.PATIENT, ResourceType.CONDITION);
    final ResponseEntity<String> response = getResponse(
        "requests/MultipleAggregationsAndResolveTest/multipleAggregationsAndResolveWithGroupings.Parameters.json");
    assertNotNull(response.getBody());
    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertJson(
        "responses/MultipleAggregationsAndResolve/multipleAggregationsAndResolveWithGroupings.Parameters.json",
        response.getBody(), JSONCompareMode.LENIENT);
  }

  ResponseEntity<String> getResponse(@Nonnull final String requestResourcePath)
      throws URISyntaxException {
    final String request = getResourceAsString(
        requestResourcePath);
    final String uri = "http://localhost:" + port + "/fhir/Patient/$aggregate";
    return restTemplate
        .exchange(uri, HttpMethod.POST,
            RequestEntity.post(new URI(uri)).header("Content-Type", "application/fhir+json")
                .body(request), String.class);
  }

}

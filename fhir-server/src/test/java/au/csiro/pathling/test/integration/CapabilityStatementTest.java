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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.json.JSONException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@TestPropertySource(properties = {
    "pathling.cors.maxAge=800",
    "pathling.cors.allowedMethods=GET,POST",
    "pathling.cors.allowedOrigins=http://foo.bar,http://boo.bar",
    "pathling.cors.allowedHeaders=X-Mine,X-Other"
})
@Tag("Tranche2")
class CapabilityStatementTest extends IntegrationTest {

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @Test
  void capabilityStatement() throws JSONException {
    final String response = restTemplate.getForObject("http://localhost:" + port + "/fhir/metadata",
        String.class);
    assertJson("responses/CapabilityStatementTest/capabilityStatement.CapabilityStatement.json",
        response, JSONCompareMode.LENIENT);
  }

  @Test
  void cors() throws JSONException {
    final HttpHeaders corsHeaders = new HttpHeaders();
    corsHeaders.setOrigin("http://foo.bar");
    corsHeaders.setAccessControlRequestMethod(HttpMethod.GET);
    corsHeaders.setAccessControlRequestHeaders(Arrays.asList("X-Mine", "X-Skip"));

    final ResponseEntity<String> response = restTemplate.exchange(
        "http://localhost:" + port + "/fhir/metadata", HttpMethod.OPTIONS,
        new HttpEntity<String>(corsHeaders), String.class);

    final HttpHeaders responseHeaders = response.getHeaders();
    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertEquals("http://foo.bar", responseHeaders.getAccessControlAllowOrigin());
    assertEquals(Arrays.asList(HttpMethod.GET, HttpMethod.POST),
        responseHeaders.getAccessControlAllowMethods());
    assertEquals(800L, responseHeaders.getAccessControlMaxAge());
    assertEquals(Collections.singletonList("X-Mine"),
        responseHeaders.getAccessControlAllowHeaders());
  }

}

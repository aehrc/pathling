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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.aggregate.AggregateExecutor;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

/**
 * @author John Grimes
 */
@Tag("Tranche2")
class SentryTest extends WireMockTest {

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @MockBean
  AggregateExecutor aggregateExecutor;

  @BeforeEach
  void setUp() {
    super.setUp();
    stubFor(post(urlPathEqualTo("/api/5513555/envelope/"))
        .willReturn(aResponse().withStatus(200)));
    when(aggregateExecutor.execute(any())).thenThrow(new RuntimeException("bar"));
  }

  @Test
  void reportsToSentry() throws URISyntaxException, InterruptedException {
    final URI uri = new URI("http", "localhost:" + port,
        "/fhir/Patient/$aggregate", "aggregation=foo",
        null);
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(uri).build(), String.class);
    final HttpStatusCode statusCode = response.getStatusCode();
    assertTrue(statusCode.is5xxServerError());

    // Give the asynchronous request sender within Sentry time to actually send the error report.
    Thread.sleep(1000);

    verify(1, postRequestedFor(urlPathEqualTo("/api/5513555/envelope/")));
  }

}

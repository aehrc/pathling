/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.aggregate.CachingAggregateExecutor;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

/**
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(locations = {"classpath:/configuration/sentry.properties"})
@Slf4j
class SentryTest {

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @Autowired
  private WireMockServer wireMockServer;

  @MockBean
  private CachingAggregateExecutor aggregateExecutor;

  @BeforeAll
  static void beforeAll() {
    // See: https://github.com/spring-projects/spring-boot/issues/21535#issuecomment-634088332
    TomcatURLStreamHandlerFactory.disable();
  }

  @BeforeEach
  void setUp() {
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
    final HttpStatus statusCode = response.getStatusCode();
    assertTrue(statusCode.is5xxServerError());

    // Give the asynchronous request sender within Sentry time to actually send the error report.
    Thread.sleep(1000);
    
    verify(1, postRequestedFor(urlPathEqualTo("/api/5513555/envelope/")));
  }

  @AfterEach
  void tearDown() {
    wireMockServer.resetAll();
  }

}
/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;

import au.csiro.pathling.async.JobRegistry;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.sparkproject.jetty.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
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
@TestPropertySource(properties = {"pathling.async.enabled=true"})
@Slf4j
class AsyncSecurityTest extends IntegrationTest {

  @MockBean
  JobRegistry jobRegistry;

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @Test
  void illegalJobId() throws URISyntaxException {
    final String uri = "http://localhost:" + port
        + "/fhir/job?id=Lorem%20ipsum%20dolor%20sit%20amet%2C%20consectetur%20adipiscing%20elit%2C%20sed%20do%20eiusmod%20tempor%20incididunt%20ut%20labore%20et%20dolore%20magna%20aliqua.%20Ut%20enim%20ad%20minim%20veniam%2C%20quis%20nostrud%20exercitation%20ullamco%20laboris%20nisi%20ut%20aliquip%20ex%20ea%20commodo%20consequat.%20Duis%20aute%20irure%20dolor%20in%20reprehenderit%20in%20voluptate%20velit%20esse%20cillum%20dolore%20eu%20fugiat%20nulla%20pariatur.%20Excepteur%20sint%20occaecat%20cupidatat%20non%20proident%2C%20sunt%20in%20culpa%20qui%20officia%20deserunt%20mollit%20anim%20id%20est%20laborum.";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    assertEquals(HttpStatus.NOT_FOUND_404, response.getStatusCode().value());
    verifyNoInteractions(jobRegistry);
  }

}

/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.Tag;
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
@Tag("Tranche2")
class ExtractTest extends IntegrationTest {

  @Autowired
  SparkSession spark;

  @Autowired
  IParser jsonParser;

  @MockBean
  Database database;

  @LocalServerPort
  int port;

  @Autowired
  TestRestTemplate restTemplate;

  @Test
  void extract() throws URISyntaxException, MalformedURLException {
    TestHelpers.mockResource(database, spark, ResourceType.DIAGNOSTICREPORT);
    final String uri = "http://localhost:" + port + "/fhir/DiagnosticReport/$extract?column=id";
    final ResponseEntity<String> response = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri)).build(), String.class);
    assertTrue(response.getStatusCode().is2xxSuccessful());
    final Parameters result = (Parameters) jsonParser.parseResource(response.getBody());
    final URL url = new URL(((UrlType) result.getParameter("url")).getValueAsString());

    final String actual;
    try {
      final InputStream expectedStream = url.openStream();
      final StringWriter writer = new StringWriter();
      IOUtils.copy(expectedStream, writer, UTF_8);
      actual = writer.toString();
    } catch (final IOException e) {
      throw new RuntimeException("Problem retrieving extract result", e);
    }

    final String expected = getResourceAsString("responses/ExtractTest/extract.csv");
    assertEquals(expected, actual);
  }

}

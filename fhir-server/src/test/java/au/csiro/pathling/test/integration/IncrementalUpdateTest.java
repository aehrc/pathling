/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsString;
import static au.csiro.pathling.test.helpers.TestHelpers.mockResourceReader;
import static org.apache.spark.sql.functions.asc;
import static org.junit.jupiter.api.Assertions.*;

import au.csiro.pathling.io.ResourceReader;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

public class IncrementalUpdateTest extends IntegrationTest {

  @Autowired
  SparkSession spark;

  @MockBean
  ResourceReader resourceReader;

  @Autowired
  ResourceWriter resourceWriter;

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;


  @BeforeEach
  void setUp() {
    mockResourceReader(resourceReader, spark, ResourceType.PATIENT);
    Dataset<Row> initialDataset = resourceReader.read(ResourceType.PATIENT);
    initialDataset
            .write()
            .mode(SaveMode.Overwrite)
            .parquet("src/test/resources/tmp/IncrementalUpdateTest/singleCreate/Patient.parquet");
    System.out.println("Initial patient count @ setUp: " + initialDataset.count());
  }

  @Test
  void singleCreate() throws URISyntaxException {
    mockResourceReader(resourceReader, spark, ResourceType.PATIENT);
    final ResponseEntity<String> response = getResponseCreateUpdate(
            "requests/IncrementalUpdateTest/singleCreate.json");
    assertNotNull(response.getBody());
    assertTrue(response.getStatusCode().is2xxSuccessful());

    Dataset<Row> updatedDataset = resourceReader.read(ResourceType.PATIENT);
    assertEquals(19, updatedDataset.count());

    // update initial dataset back to data warehouse after test
    Dataset<Row> initialDataset = spark.read().parquet("src/test/resources/tmp/IncrementalUpdateTest/singleCreate/Patient.parquet");
    resourceWriter.update(resourceReader, ResourceType.PATIENT, initialDataset);
    System.out.println("Initial patient count @ teardown: " + initialDataset.count());
  }

  private ResponseEntity<String> getResponseCreateUpdate(@Nonnull final String requestResourcePath)
          throws URISyntaxException {
    final String request = getResourceAsString(
            requestResourcePath);
    final String uri = "http://localhost:" + port + "/fhir/Patient";
    return restTemplate
            .exchange(uri, HttpMethod.POST,
                    RequestEntity.post(new URI(uri)).header("Content-Type", "application/fhir+json")
                            .body(request), String.class);
  }

  @AfterEach
  void tearDown() {
    try {
      FileUtils.cleanDirectory(new File("src/test/resources/tmp"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

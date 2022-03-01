/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.helpers.TestHelpers.getParquetPathForResourceType;
import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsString;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.parser.IParser;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
    "pathling.storage.warehouseUrl=" + IntegrationTest.INDIVIDUAL_TEST_WAREHOUSE,
    "pathling.storage.databaseName=CreateTest"
})
public class CreateTest extends IntegrationTest {

  @Autowired
  SparkSession spark;

  @Autowired
  IParser jsonParser;

  @Autowired
  ResourceReader resourceReader;

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  public static final String TEST_DB = INDIVIDUAL_TEST_WAREHOUSE + "/CreateTest";
  public static final String PARQUET_PATH = TEST_DB + "/Patient.parquet";
  public static final MediaType FHIR_MEDIA_TYPE = new MediaType("application", "fhir+json");
  public static final CustomComparator ID_BLIND_COMPARATOR = new CustomComparator(
      JSONCompareMode.LENIENT, new Customization("id", (o1, o2) -> true));

  @BeforeEach
  void setUp() throws IOException {
    copyFolder(new File(getParquetPathForResourceType(ResourceType.PATIENT)).toPath(),
        new File(PARQUET_PATH).toPath());
    resourceReader.updateAvailableResourceTypes();
  }

  @Test
  void create() throws URISyntaxException {
    // Check the total Patient count.
    final int expectedCount = Math.toIntExact(resourceReader.read(ResourceType.PATIENT).count());
    assertPatientCount(expectedCount);

    // Send a create request with a new Patient resource.
    final String request = getResourceAsString("requests/CreateTest/create.Patient.json");
    final String url = "http://localhost:" + port + "/fhir/Patient";
    final ResponseEntity<String> response = restTemplate
        .exchange(url, HttpMethod.POST, RequestEntity.post(new URI(url))
            .contentType(FHIR_MEDIA_TYPE)
            .accept(FHIR_MEDIA_TYPE)
            .body(request), String.class);
    assertEquals(201, response.getStatusCode().value());
    assertNotNull(response.getBody());
    JSONAssert.assertEquals(request, response.getBody(), ID_BLIND_COMPARATOR);

    // Get the new patient resource via search and verify its contents.
    final Patient patient = (Patient) jsonParser.parseResource(response.getBody());
    final String patientId = patient.getIdElement().getIdPart().replace("Patient/", "");
    final String searchUrl =
        "http://localhost:" + port + "/fhir/Patient?_query=fhirPath&filter=id+=+'"
            + patientId + "'";
    final ResponseEntity<String> searchResponse = restTemplate
        .exchange(searchUrl, HttpMethod.GET, RequestEntity.get(new URI(searchUrl))
            .accept(FHIR_MEDIA_TYPE)
            .build(), String.class);
    assertTrue(searchResponse.getStatusCode().is2xxSuccessful());
    assertNotNull(searchResponse.getBody());
    final Bundle searchBundle = (Bundle) jsonParser.parseResource(searchResponse.getBody());
    assertEquals(1, searchBundle.getTotal());
    assertEquals(1, searchBundle.getEntry().size());
    final BundleEntryComponent bundleEntryComponent = searchBundle.getEntry().get(0);
    assertEquals(patientId, bundleEntryComponent.getResource().getIdElement().getIdPart());

    // Check that the new Patient count is now one more than it was previously.
    assertPatientCount(expectedCount + 1);
  }

  private void assertPatientCount(final int expectedCount) throws URISyntaxException {
    final String uri = "http://localhost:" + port + "/fhir/Patient?_summary=count";
    final ResponseEntity<String> countResponse1 = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri))
            .accept(FHIR_MEDIA_TYPE).build(), String.class);
    final Bundle countBundle1 = (Bundle) jsonParser.parseResource(countResponse1.getBody());
    assertEquals(expectedCount, countBundle1.getTotal());
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.cleanDirectory(new File(TEST_DB));
  }

  private static void copyFolder(@Nonnull final Path src, @Nonnull final Path dest)
      throws IOException {
    try (final Stream<Path> stream = Files.walk(src)) {
      stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
    }
  }

  private static void copy(@Nonnull final Path source, @Nonnull final Path dest) {
    try {
      Files.copy(source, dest, REPLACE_EXISTING);
    } catch (final Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}

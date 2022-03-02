/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.helpers.TestHelpers.getParquetPathForResourceType;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

public abstract class ModificationTest extends IntegrationTest {

  @LocalServerPort
  protected int port;

  @Autowired
  protected TestRestTemplate restTemplate;

  @Autowired
  SparkSession spark;

  @Autowired
  IParser jsonParser;

  @Autowired
  ResourceReader resourceReader;

  public static final MediaType FHIR_MEDIA_TYPE = new MediaType("application", "fhir+json");

  protected abstract String getTestName();

  protected String getTestDatabase() {
    return INDIVIDUAL_TEST_WAREHOUSE + "/" + getTestName();
  }

  protected String getParquetPath() {
    return getTestDatabase() + "/Patient.parquet";
  }

  @BeforeEach
  void setUp() throws IOException {
    //noinspection ResultOfMethodCallIgnored
    new File(getTestDatabase()).mkdirs();
    ModificationTest.copyFolder(
        new File(getParquetPathForResourceType(ResourceType.PATIENT)).toPath(),
        new File(getParquetPath()).toPath());
    resourceReader.updateAvailableResourceTypes();
  }

  protected void assertPatientCount(final int expectedCount) throws URISyntaxException {
    final String uri = "http://localhost:" + port + "/fhir/Patient?_summary=count";
    final ResponseEntity<String> countResponse1 = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri))
            .accept(FHIR_MEDIA_TYPE).build(), String.class);
    final Bundle countBundle1 = (Bundle) jsonParser.parseResource(countResponse1.getBody());
    assertEquals(expectedCount, countBundle1.getTotal());
  }

  @Nonnull
  protected BundleEntryComponent getPatientResult(@Nonnull final String id)
      throws URISyntaxException {
    final String searchUrl =
        "http://localhost:" + port + "/fhir/Patient?_query=fhirPath&filter=id+=+'"
            + id + "'";
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
    assertEquals(id, bundleEntryComponent.getResource().getIdElement().getIdPart());
    return bundleEntryComponent;
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.cleanDirectory(new File(getTestDatabase()));
  }

  private static void copyFolder(@Nonnull final Path src, @Nonnull final Path dest)
      throws IOException {
    try (final Stream<Path> stream = Files.walk(src)) {
      stream.forEach(source -> ModificationTest.copy(source, dest.resolve(src.relativize(source))));
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

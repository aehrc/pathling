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

package au.csiro.pathling.test.integration.modification;

import static au.csiro.pathling.test.helpers.TestHelpers.getDatabaseUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.test.integration.IntegrationTest;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.FileSystemUtils;

@TestPropertySource(
    properties = {"pathling.storage.databaseName=default"})
@Tag("Tranche1")
@Slf4j
abstract class ModificationTest extends IntegrationTest {

  @LocalServerPort
  protected int port;

  @Autowired
  TestRestTemplate restTemplate;

  @Autowired
  IParser jsonParser;

  @Autowired
  ServerConfiguration configuration;

  @Autowired
  SparkSession spark;

  @TempDir
  static File tempDirectory;

  File databaseDirectory;

  @DynamicPropertySource
  @SuppressWarnings("unused")
  static void registerProperties(@Nonnull final DynamicPropertyRegistry registry) {
    registry.add("pathling.storage.warehouseUrl",
        () -> "file://" + tempDirectory.toString().replaceFirst("/$", ""));
  }

  @BeforeEach
  void setUp() throws IOException {
    databaseDirectory = new File(
        configuration.getStorage().getWarehouseUrl().replaceFirst("file://", ""), "default");
    final Path source = Path.of(getDatabaseUrl().replaceFirst("file://", ""));
    final Path destination = databaseDirectory.toPath();
    log.debug("Copying test data from {} to {}", source, destination);
    assertTrue(databaseDirectory.mkdirs());
    FileSystemUtils.copyRecursively(source, destination);
  }

  @AfterEach
  void tearDown() throws Exception {
    log.debug("Deleting directory: {}", databaseDirectory);
    assertTrue(FileSystemUtils.deleteRecursively(databaseDirectory.toPath()));
    spark.sqlContext().clearCache();
  }

  void assertResourceCount(@Nonnull final ResourceType resourceType,
      final int expectedCount) throws URISyntaxException {
    final String uri =
        "http://localhost:" + port + "/fhir/" + resourceType.toCode() + "?_summary=count";
    final ResponseEntity<String> countResponse = restTemplate
        .exchange(uri, HttpMethod.GET, RequestEntity.get(new URI(uri))
            .accept(TestHelpers.FHIR_MEDIA_TYPE).build(), String.class);
    final Bundle countBundle = (Bundle) jsonParser.parseResource(countResponse.getBody());
    assertEquals(expectedCount, countBundle.getTotal());
  }

  @Nonnull
  BundleEntryComponent getResourceResult(@Nonnull final ResourceType resourceType,
      @Nonnull final String id) throws URISyntaxException {
    final String searchUrl = "http://localhost:" + port + "/fhir/" + resourceType.toCode()
        + "?_query=fhirPath&filter=id+=+'" + id + "'";
    final ResponseEntity<String> searchResponse = restTemplate
        .exchange(searchUrl, HttpMethod.GET, RequestEntity.get(new URI(searchUrl))
            .accept(TestHelpers.FHIR_MEDIA_TYPE)
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

}

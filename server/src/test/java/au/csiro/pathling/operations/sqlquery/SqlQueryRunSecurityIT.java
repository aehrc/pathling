/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.sqlquery;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests covering security guarantees of the {@code $sqlquery-run} pipeline. Confirms
 * that requests using the datasource short-name syntax to read arbitrary local files are rejected
 * with a 4xx response, and that a known-good baseline still succeeds.
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class SqlQueryRunSecurityIT {

  private static final Gson GSON = new Gson();

  private static final String BASELINE_SQL =
      "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)";

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
    // Use a deliberately small cap so the test can demonstrate clamping without depending on
    // the production default.
    registry.add("pathling.sqlQuery.maxRows", () -> "2");
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build();
    jsonParser = fhirContext.newJsonParser();
  }

  @Test
  void rejectsCsvShortNameFileRead() {
    final String body =
        postExpect4xx(parametersJson(sqlQueryLibrary("SELECT * FROM csv.`/etc/passwd`")));
    assertThat(body).contains("undeclared table");
  }

  @Test
  void rejectsParquetWarehouseRead() {
    final String body =
        postExpect4xx(
            parametersJson(
                sqlQueryLibrary(
                    "SELECT id, gender FROM"
                        + " parquet.`/Users/gri306/Warehouse/default/Patient.parquet`")));
    assertThat(body).contains("undeclared table");
  }

  @Test
  void baselineHappyPathStillWorks() {
    final String body = postOk(parametersJson(sqlQueryLibrary(BASELINE_SQL)));
    assertThat(body).contains("alice").contains("bob");
  }

  @Test
  void rejectsTvfDosAttack() {
    // The literal exploit recorded in the threat model: a Cartesian product over two range TVFs.
    // Must be rejected by the validator before any work is scheduled.
    final String body =
        postExpect4xx(
            parametersJson(
                sqlQueryLibrary(
                    "SELECT count(*) FROM range(0, 1000000) a CROSS JOIN range(0, 1000000) b")));
    assertThat(body).contains("disallowed plan node");
  }

  @Test
  void enforcesServerRowCap() {
    // The configured cap is 2; the source has 5 rows. Only 2 should make it through.
    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)")));
    final long rowCount = body.lines().filter(line -> !line.isBlank()).count();
    assertThat(rowCount).isEqualTo(2L);
  }

  @Nonnull
  private String postOk(@Nonnull final String body) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  @Nonnull
  private String postExpect4xx(@Nonnull final String body) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sqlquery-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .is4xxClientError()
            .expectBody()
            .returnResult();
    final byte[] payload = result.getResponseBodyContent();
    return payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
  }

  @Nonnull
  private Library sqlQueryLibrary(@Nonnull final String sql) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    return library;
  }

  @Nonnull
  private String parametersJson(@Nonnull final Library library) {
    final String libraryJson = jsonParser.encodeResourceToString(library);
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new java.util.ArrayList<>();

    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "queryResource");
    queryResourceParam.put("resource", GSON.fromJson(libraryJson, Map.class));
    parameterList.add(queryResourceParam);

    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", SqlQueryOutputFormat.NDJSON.getCode());
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return GSON.toJson(parameters);
  }
}

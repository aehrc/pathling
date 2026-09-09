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

package au.csiro.pathling.operations.sql;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import au.csiro.pathling.operations.sqlquery.SqlViewTestConfiguration;
import au.csiro.pathling.util.DirectoryCleanup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;

/**
 * End-to-end integration test for operator-configured external tables in SQL on FHIR queries (spec
 * 060). Follows the scenarios of the feature's quickstart: a SQLQuery joins a stored ViewDefinition
 * to a Delta table and to a Parquet table, reads a table on its own, describes it, and is still
 * refused when it names the table by anything other than its declared label.
 *
 * <p>Backed by {@link SqlViewTestConfiguration} for the stored FHIR artefacts and data, and by two
 * small tables written into a temporary directory before the server reads them. Four tables are
 * configured: the Delta and Parquet cohorts tables, a table whose URL collides with a stored
 * SQLView, and a table whose path is never written.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock("wiremock")
@ActiveProfiles({"integration-test"})
@Import(SqlViewTestConfiguration.class)
class SqlExternalTableIT extends AbstractAsyncExportIT {

  /** The configured URL of the Delta cohorts table. */
  static final String COHORTS_DELTA_URL = "https://example.org/data/cohorts";

  /** The configured URL of the Parquet cohorts table. */
  static final String COHORTS_PARQUET_URL = "https://example.org/data/cohorts-parquet";

  /** A configured table URL that is also the URL of a stored SQLView. */
  static final String AMBIGUOUS_URL =
      SqlViewTestConfiguration.libraryUrl(SqlViewTestConfiguration.ACTIVE_PATIENTS_ID);

  /** A configured table URL whose path is never written. */
  static final String MISSING_URL = "https://example.org/data/missing";

  /** The directory name under the temp directory of the path that is never written. */
  static final String MISSING_DIRECTORY = "does-not-exist";

  @TempDir static Path tablesDir;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);

    registry.add("pathling.sqlQuery.externalTables.0.url", () -> COHORTS_DELTA_URL);
    registry.add("pathling.sqlQuery.externalTables.0.path", () -> cohortsDeltaPath());
    registry.add("pathling.sqlQuery.externalTables.1.url", () -> COHORTS_PARQUET_URL);
    registry.add("pathling.sqlQuery.externalTables.1.path", () -> cohortsParquetPath());
    registry.add("pathling.sqlQuery.externalTables.1.format", () -> "parquet");
    registry.add("pathling.sqlQuery.externalTables.2.url", () -> AMBIGUOUS_URL);
    registry.add("pathling.sqlQuery.externalTables.2.path", () -> cohortsDeltaPath());
    registry.add("pathling.sqlQuery.externalTables.3.url", () -> MISSING_URL);
    registry.add(
        "pathling.sqlQuery.externalTables.3.path",
        () -> "file://" + tablesDir.resolve(MISSING_DIRECTORY).toAbsolutePath());
  }

  @Nonnull
  static String cohortsDeltaPath() {
    return "file://" + tablesDir.resolve("cohorts_delta").toAbsolutePath();
  }

  @Nonnull
  static String cohortsParquetPath() {
    return "file://" + tablesDir.resolve("cohorts_parquet").toAbsolutePath();
  }

  /**
   * Writes the Delta and Parquet fixtures once the temp directory exists. The Spark session is
   * resolved as a parameter because a static lifecycle method cannot see the autowired fields.
   */
  @BeforeAll
  static void writeTables(@Autowired final SparkSession sparkSession) {
    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("family_name", DataTypes.StringType, false),
              DataTypes.createStructField("cohort", DataTypes.StringType, false)
            });
    final List<Row> rows =
        List.of(RowFactory.create("Smith", "A"), RowFactory.create("Williams", "B"));
    sparkSession.createDataFrame(rows, schema).write().format("delta").save(cohortsDeltaPath());
    sparkSession.createDataFrame(rows, schema).write().format("parquet").save(cohortsParquetPath());
  }

  @AfterAll
  static void cleanupAll() throws IOException {
    // Clean the temp directory before JUnit's @TempDir cleanup runs, so Spark/Delta file handles do
    // not prevent directory deletion.
    DirectoryCleanup.cleanDirectoryTolerantly(tablesDir);
  }

  @BeforeEach
  void setUpParser() {
    jsonParser = fhirContext.newJsonParser();
  }

  // -------------------------------------------------------------------------
  // Scenarios 1 and 2: a FHIR view joined to the Delta and the Parquet table.
  // -------------------------------------------------------------------------

  @Test
  void joinsAFhirViewToTheDeltaTable() {
    final String body = postOk(parametersJson(joinQuery(COHORTS_DELTA_URL)));

    assertThat(rowsOf(body, "id", "cohort")).containsExactly("p1/A", "p3/B");
  }

  @Test
  void joinsAFhirViewToTheParquetTable() {
    final String body = postOk(parametersJson(joinQuery(COHORTS_PARQUET_URL)));

    assertThat(rowsOf(body, "id", "cohort")).containsExactly("p1/A", "p3/B");
  }

  // -------------------------------------------------------------------------
  // Scenario 3: the table on its own, selected and described.
  // -------------------------------------------------------------------------

  @Test
  void selectsFromTheTableAlone() {
    final Library library =
        sqlQueryLibrary(
            "SELECT family_name, cohort FROM cohort ORDER BY family_name",
            Map.of("cohort", COHORTS_DELTA_URL));

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "family_name", "cohort")).containsExactly("Smith/A", "Williams/B");
  }

  @Test
  void describesTheTable() {
    final Library library = sqlQueryLibrary("DESCRIBE cohort", Map.of("cohort", COHORTS_DELTA_URL));

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "col_name", "data_type"))
        .containsExactlyInAnyOrder("family_name/string", "cohort/string");
  }

  // -------------------------------------------------------------------------
  // Scenario 4: naming the table by anything other than its label is refused.
  // -------------------------------------------------------------------------

  @Test
  void rejectsNamingTheTableByPath() {
    final Library library =
        sqlQueryLibrary(
            "SELECT * FROM parquet.`" + cohortsParquetPath() + "`",
            Map.of("cohort", COHORTS_PARQUET_URL));

    final String body = postExpectStatus(parametersJson(library), 400);

    assertThat(body).contains("SQL references an undeclared table");
  }

  // -------------------------------------------------------------------------
  // Request helpers
  // -------------------------------------------------------------------------

  /** Builds the Scenario 1 SQLQuery joining the stored Patient view to the given cohorts table. */
  @Nonnull
  Library joinQuery(@Nonnull final String cohortsUrl) {
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("patient", SqlViewTestConfiguration.PATIENT_VIEW_URL);
    dependencies.put("cohort", cohortsUrl);
    return sqlQueryLibrary(
        "SELECT p.id, c.cohort FROM patient p JOIN cohort c ON c.family_name = p.family_name"
            + " ORDER BY p.id",
        dependencies);
  }

  /**
   * Projects each NDJSON row of a response body to {@code <first>/<second>} using the two named
   * columns, in response order.
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  List<String> rowsOf(
      @Nonnull final String body, @Nonnull final String first, @Nonnull final String second) {
    return Arrays.stream(body.trim().split("\n"))
        .map(line -> (Map<String, Object>) gson.fromJson(line, Map.class))
        .map(row -> row.get(first) + "/" + row.get(second))
        .toList();
  }

  @Nonnull
  String postOk(@Nonnull final String body) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sql-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(
                MediaType.parseMediaType(SqlQueryOutputFormat.NDJSON.getContentType()))
            .expectBody()
            .returnResult();
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }

  @Nonnull
  String postExpectStatus(@Nonnull final String body, final int status) {
    final EntityExchangeResult<byte[]> result =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sql-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isEqualTo(status)
            .expectBody()
            .returnResult();
    final byte[] payload = result.getResponseBodyContent();
    return payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
  }

  /** Builds an inline SQLQuery Library with the given depends-on dependencies (label to URL). */
  @Nonnull
  Library sqlQueryLibrary(
      @Nonnull final String sql, @Nonnull final Map<String, String> dependenciesByLabel) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    new LinkedHashMap<>(dependenciesByLabel)
        .forEach(
            (label, resource) ->
                library.addRelatedArtifact(
                    new RelatedArtifact()
                        .setType(RelatedArtifactType.DEPENDSON)
                        .setLabel(label)
                        .setResource(resource)));
    return library;
  }

  /** Wraps the Library as the {@code subjectResource} of a {@code $sql-run} Parameters body. */
  @Nonnull
  String parametersJson(@Nonnull final Library library) {
    final String libraryJson = jsonParser.encodeResourceToString(library);
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    final Map<String, Object> queryResourceParam = new LinkedHashMap<>();
    queryResourceParam.put("name", "subjectResource");
    queryResourceParam.put("resource", gson.fromJson(libraryJson, Map.class));
    parameterList.add(queryResourceParam);

    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", SqlQueryOutputFormat.NDJSON.getCode());
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return gson.toJson(parameters);
  }
}

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
import au.csiro.pathling.terminology.local.CodeSystemEntry;
import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import au.csiro.pathling.test.Rf2Mini;
import au.csiro.pathling.util.DirectoryCleanup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.junit.jupiter.api.AfterAll;
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
 * End-to-end integration test for value set dependencies in SQL on FHIR queries (spec 061), in
 * LOCAL terminology mode over a store built from the {@code rf2-mini} SNOMED CT fixture. Covers
 * User Story 1 scenario 3: the relation carries the SNOMED CT system, the store's version URI, the
 * stored displays and the inactive flag, and a semi-join to a SNOMED CT implicit value set keeps
 * the expected conditions. Covers User Story 2 scenario 5: a {@code context} ValueSet carrying only
 * a compose is evaluated by the store.
 *
 * <p>The store is imported once per class into a temporary directory before the application context
 * starts, since the local terminology service opens the store at context creation. The fixture
 * release is extracted from the {@code terminology} test-jar, whose resources are not directly
 * addressable as a filesystem path. The Spark session used for the import is built with the same
 * Delta extensions the server configures, so the server's own {@code getOrCreate} reuses it rather
 * than competing with it.
 *
 * <p>Backed by {@link SqlValueSetTestConfiguration} for the stored ViewDefinition and the Condition
 * data.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock("wiremock")
@ActiveProfiles({"integration-test"})
@Import(SqlValueSetTestConfiguration.class)
class SqlValueSetLocalIT extends AbstractAsyncExportIT {

  /** The SNOMED CT implicit value set of the descendants of type 2 diabetes, inclusive. */
  static final String TYPE2_DIABETES_URL =
      Rf2Mini.SNOMED_URI + "?fhir_vs=isa/" + Rf2Mini.TYPE2_DIABETES;

  /** A VCL implicit value set selecting the inactive concepts of the store. */
  static final String INACTIVE_URL =
      "http://fhir.org/VCL?v1="
          + URLEncoder.encode(
              "(" + Rf2Mini.SNOMED_URI + ")inactive = true", StandardCharsets.UTF_8);

  @TempDir static Path storeDir;

  private static ConceptDictionary dictionary;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
    final String storagePath = importStore();
    registry.add("pathling.terminology.mode", () -> "LOCAL");
    registry.add("pathling.terminology.local.storagePath", () -> storagePath);
  }

  /**
   * Imports the base rf2-mini release into a store under the temp directory and opens its
   * dictionary, so the test can look up the displays the relation is expected to carry.
   *
   * <p>Tomcat's URL stream handler factory is registered before Spark starts. Spark installs
   * Hadoop's factory into the JVM-wide slot when it can, and tolerates the slot being taken; Tomcat
   * does not, and would refuse to start once the server context creates the web server (the
   * SPARK-25694 conflict). In production the web server starts before the Spark session, so the
   * order here restores the order the server itself sees.
   */
  @Nonnull
  private static String importStore() {
    final Path release = extractRelease("international-20230601");
    final String storagePath = storeDir.resolve("store").toString();
    TomcatURLStreamHandlerFactory.register();
    final SparkSession spark =
        SparkSession.builder()
            .appName("SqlValueSetLocalIT")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    new SnomedRf2Importer(spark, storagePath).importFrom(release.toString(), null);
    final TerminologyStoreReader reader = TerminologyStoreReader.open(storagePath, Map.of());
    final String systemVersionId =
        CodeSystemEntry.loadCatalogue(reader).stream()
            .filter(entry -> Rf2Mini.VERSION_20230601.equals(entry.getVersion()))
            .map(CodeSystemEntry::getSystemVersionId)
            .findFirst()
            .orElseThrow();
    dictionary = CodeSystemIndexes.load(reader, systemVersionId).dictionary();
    return storagePath;
  }

  /**
   * Copies a release directory of the rf2-mini fixture from the test classpath to the temp
   * directory. The fixture ships inside the {@code terminology} test-jar, so its entries are read
   * through a jar filesystem; a release present on disk is copied directly.
   */
  @Nonnull
  private static Path extractRelease(@Nonnull final String release) {
    final URL url =
        Objects.requireNonNull(
            Rf2Mini.class.getResource("/rf2-mini/" + release),
            "rf2-mini release not found on the classpath: " + release);
    final Path target = storeDir.resolve(release);
    try {
      final URI uri = url.toURI();
      if ("jar".equals(uri.getScheme())) {
        try (FileSystem jar = FileSystems.newFileSystem(uri, Map.of())) {
          copyTree(jar.getPath("/rf2-mini/" + release), target);
        }
      } else {
        copyTree(Path.of(uri), target);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("Unable to extract the rf2-mini release " + release, e);
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("Unable to resolve the rf2-mini release " + release, e);
    }
    return target;
  }

  /** Copies every file beneath {@code source} to the same relative path beneath {@code target}. */
  private static void copyTree(@Nonnull final Path source, @Nonnull final Path target)
      throws IOException {
    try (Stream<Path> paths = Files.walk(source)) {
      for (final Path path : paths.toList()) {
        final Path destination = target.resolve(source.relativize(path).toString());
        if (Files.isDirectory(path)) {
          Files.createDirectories(destination);
        } else {
          Files.createDirectories(destination.getParent());
          Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  @AfterAll
  static void cleanupAll() throws IOException {
    // Clean the temp directory before JUnit's @TempDir cleanup runs, so Spark/Delta file handles do
    // not prevent directory deletion.
    DirectoryCleanup.cleanDirectoryTolerantly(storeDir);
  }

  @BeforeEach
  void setUpParser() {
    jsonParser = fhirContext.newJsonParser();
  }

  // -------------------------------------------------------------------------
  // Scenario 3: the relation carries the system, the store's version, displays and inactive.
  // -------------------------------------------------------------------------

  @Test
  void selectingFromAnImplicitValueSetReturnsTheStoreComputedMembers() {
    final Library library =
        sqlQueryLibrary(
            "SELECT system, version, code, display, inactive FROM t2dm ORDER BY code",
            Map.of("t2dm", TYPE2_DIABETES_URL));

    final String body = postOk(parametersJson(library));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows)
        .extracting(row -> row.get("code"))
        .containsExactly(Rf2Mini.TYPE2_DIABETES, Rf2Mini.TYPE2_WITH_COMPLICATION);
    for (final Map<String, Object> row : rows) {
      assertThat(row).containsEntry("system", Rf2Mini.SNOMED_URI);
      assertThat(row).containsEntry("version", Rf2Mini.VERSION_20230601);
      assertThat(row.get("display")).isEqualTo(display((String) row.get("code")));
      assertThat(row.get("inactive")).isNull();
    }
  }

  @Test
  void inactiveMembersCarryTrueInTheInactiveColumn() {
    final Library library =
        sqlQueryLibrary(
            "SELECT system, version, code, display, inactive FROM inactive_codes",
            Map.of("inactive_codes", INACTIVE_URL));

    final String body = postOk(parametersJson(library));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .containsEntry("system", Rf2Mini.SNOMED_URI)
        .containsEntry("version", Rf2Mini.VERSION_20230601)
        .containsEntry("code", Rf2Mini.DIABETES_INACTIVE)
        .containsEntry("display", display(Rf2Mini.DIABETES_INACTIVE))
        .containsEntry("inactive", Boolean.TRUE);
  }

  @Test
  void semiJoinReturnsTheConditionsWhoseCodeTheStoreComputesAsMembers() {
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("conditions", SqlValueSetTestConfiguration.CONDITION_VIEW_URL);
    dependencies.put("t2dm", TYPE2_DIABETES_URL);
    final Library library =
        sqlQueryLibrary(
            "SELECT conditions.patient_id, conditions.code FROM conditions"
                + " WHERE EXISTS (SELECT 1 FROM t2dm"
                + " WHERE t2dm.system = conditions.system AND t2dm.code = conditions.code)"
                + " ORDER BY conditions.id",
            dependencies);

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "patient_id", "code"))
        .containsExactly(
            "Patient/p1/" + Rf2Mini.TYPE2_DIABETES,
            "Patient/p2/" + Rf2Mini.TYPE2_WITH_COMPLICATION);
  }

  // -------------------------------------------------------------------------
  // US2 scenario 5: a compose-only supplied ValueSet is evaluated by the store.
  // -------------------------------------------------------------------------

  @Test
  void composeOnlySuppliedValueSetIsEvaluatedByTheStore() {
    final String url = "http://example.org/ValueSet/supplied-compose";
    final ValueSet supplied = new ValueSet();
    supplied.setUrl(url);
    final ConceptSetComponent include = supplied.getCompose().addInclude();
    include.setSystem(Rf2Mini.SNOMED_URI);
    include.addConcept().setCode(Rf2Mini.HYPERTENSION);
    include.addConcept().setCode(Rf2Mini.TYPE2_DIABETES);
    final Library library =
        sqlQueryLibrary(
            "SELECT system, version, code, display, inactive FROM supplied_codes ORDER BY code",
            Map.of("supplied_codes", url));

    final String body =
        postOk(parametersJson(library, resourcePart("context", resourceMap(supplied))));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows)
        .extracting(row -> row.get("code"))
        .containsExactly(Rf2Mini.TYPE2_DIABETES, Rf2Mini.HYPERTENSION);
    for (final Map<String, Object> row : rows) {
      assertThat(row).containsEntry("system", Rf2Mini.SNOMED_URI);
      assertThat(row).containsEntry("version", Rf2Mini.VERSION_20230601);
      assertThat(row.get("display")).isEqualTo(display((String) row.get("code")));
      assertThat(row.get("inactive")).isNull();
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Looks up the display the store holds for a concept. */
  @Nonnull
  private static String display(@Nonnull final String code) {
    return Objects.requireNonNull(
        dictionary.display(Objects.requireNonNull(dictionary.denseId(code))));
  }

  /** Parses each NDJSON row of a response body into a map, in response order. */
  @Nonnull
  @SuppressWarnings("unchecked")
  List<Map<String, Object>> rows(@Nonnull final String body) {
    return Arrays.stream(body.trim().split("\n"))
        .map(line -> (Map<String, Object>) gson.fromJson(line, Map.class))
        .toList();
  }

  /**
   * Projects each NDJSON row of a response body to {@code <first>/<second>} using the two named
   * columns, in response order.
   */
  @Nonnull
  List<String> rowsOf(
      @Nonnull final String body, @Nonnull final String first, @Nonnull final String second) {
    return rows(body).stream().map(row -> row.get(first) + "/" + row.get(second)).toList();
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

  /** Encodes a resource as the generic JSON map the Gson-built request bodies carry. */
  @Nonnull
  @SuppressWarnings("unchecked")
  Map<String, Object> resourceMap(@Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    return gson.fromJson(jsonParser.encodeResourceToString(resource), Map.class);
  }

  /**
   * Wraps the Library as the {@code subjectResource} of a {@code $sql-run} Parameters body, along
   * with the NDJSON format and any further parameters.
   */
  @SafeVarargs
  @Nonnull
  final String parametersJson(
      @Nonnull final Library library, @Nonnull final Map<String, Object>... extraParameters) {
    final Map<String, Object> parameters =
        parameters(
            resourcePart("subjectResource", resourceMap(library)),
            simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode()));
    for (final Map<String, Object> extra : extraParameters) {
      addParam(parameters, extra);
    }
    return gson.toJson(parameters);
  }
}

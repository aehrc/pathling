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

import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.ACUTE_MYOCARDIAL_INFARCTION;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.DIABETES_MELLITUS;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.FIT_AND_WELL;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.ICD10;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.MYOCARDIAL_INFARCTION;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.SCT_TO_ICD10_URL;
import static au.csiro.pathling.operations.sql.SqlConceptMapTestConfiguration.SNOMED;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import au.csiro.pathling.terminology.store.FhirTerminologyImporter;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.test.Rf2Mini;
import au.csiro.pathling.util.DirectoryCleanup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ValueSet;
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
 * End-to-end integration test for concept map dependencies in SQL on FHIR queries (spec 062), in
 * LOCAL terminology mode over a store built from the {@code rf2-mini} SNOMED CT fixture with the
 * worked example concept map imported. Covers User Story 1 scenario 4: the left join and the
 * relation itself give the same rows as in SERVER mode. Covers scenarios 5 and 6: a pinned
 * reference reads that version and an unpinned one the latest. Covers scenario 14: a pinned
 * reference to a concept map, or to a value set, imported at a canonical URL that carries a query
 * resolves as that artefact. Covers User Story 4 scenarios 1-5: each of the four SNOMED CT implicit
 * concept maps holds one row per association row, with the store's displays and the relationship
 * THO defines; a concept with three POSSIBLY EQUIVALENT TO targets has three rows; the version
 * comes from an edition/version base or, for a bare base, is the store's default; and a reference
 * set outside the four is not found.
 *
 * <p>The store is imported once per class into a temporary directory before the application context
 * starts, since the local terminology service opens the store at context creation. Both fixture
 * releases ship one REPLACED BY, POSSIBLY EQUIVALENT TO and ALTERNATIVE source concept beside their
 * SAME AS rows. The store holds a copy of the base release whose association reference set gains
 * further REPLACED BY, POSSIBLY EQUIVALENT TO and ALTERNATIVE rows, the unmodified later release
 * (the store's default version, holding only the fixture's rows), the worked example as versions
 * {@code 2026} and {@code 2025} (the latter with an edited diabetes target, so that the rows show
 * which version was read), a copy of version {@code 2026} at a canonical URL with a query, and a
 * value set at another such URL. The fixture releases are extracted from the {@code terminology}
 * test-jar, whose resources are not directly addressable as a filesystem path.
 *
 * <p>Backed by {@link SqlConceptMapTestConfiguration} for the stored ViewDefinition and the
 * Condition data.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock("wiremock")
@ActiveProfiles({"integration-test"})
@Import(SqlConceptMapTestConfiguration.class)
class SqlConceptMapLocalIT extends AbstractAsyncExportIT {

  /** The diabetes target of the edited {@code 2025} version of the worked example. */
  static final String EDITED_2025_TARGET = "E11";

  /** The canonical URL, carrying a query, of a copy of the worked example. */
  static final String CONCEPT_MAP_WITH_QUERY_URL = SCT_TO_ICD10_URL + "?edition=au";

  /** The canonical URL, carrying a query, of a value set imported as version {@code 2026}. */
  static final String VALUE_SET_WITH_QUERY_URL = "http://example.org/ValueSet/x?edition=au";

  /** The SAME AS association reference set. */
  static final String SAME_AS = Rf2Mini.SAME_AS_REFSET;

  /** The REPLACED BY association reference set. */
  static final String REPLACED_BY = Rf2Mini.REPLACED_BY_REFSET;

  /** The POSSIBLY EQUIVALENT TO association reference set. */
  static final String POSSIBLY_EQUIVALENT_TO = Rf2Mini.POSSIBLY_EQUIVALENT_TO_REFSET;

  /** The ALTERNATIVE association reference set. */
  static final String ALTERNATIVE = Rf2Mini.ALTERNATIVE_REFSET;

  /** The MOVED FROM association reference set, for which THO defines no implicit concept map. */
  static final String MOVED_FROM = "900000000000497000";

  /** An association target that is not in the release's concept dictionary. */
  static final String ABSENT = "9999999003";

  /** The store's display of {@link Rf2Mini#TYPE2_DIABETES}. */
  static final String T2DM = "Type 2 diabetes mellitus";

  /** The store's display of {@link Rf2Mini#TYPE1_DIABETES}. */
  static final String T1DM = "Type 1 diabetes mellitus";

  /** The relationship of the SAME AS and REPLACED BY implicit maps. */
  static final String EQUIVALENT = "equivalent";

  /** The relationship of the POSSIBLY EQUIVALENT TO and ALTERNATIVE implicit maps. */
  static final String RELATED_TO = "related-to";

  /**
   * The REPLACED BY row that both fixture releases ship, and the only one the later release has.
   */
  static final String FIXTURE_REPLACED_BY_ROW =
      row(
          Rf2Mini.REPLACED_BY_SOURCE,
          "Mini diabetes subtype 1",
          Rf2Mini.TYPE1_DIABETES,
          T1DM,
          EQUIVALENT);

  @TempDir static Path storeDir;

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
   * Imports the extended copy of the base rf2-mini release, the unmodified later release and the
   * FHIR terminology resources into a store under the temp directory.
   *
   * <p>Tomcat's URL stream handler factory is registered before Spark starts, as {@link
   * SqlValueSetLocalIT} explains, so that the server context can still start its web server.
   */
  @Nonnull
  private static String importStore() {
    final Path release = extractRelease("international-20230601");
    appendAssociationRows(release);
    final Path laterRelease = extractRelease("international-20240601");
    final Path resources = writeResources();
    final String storagePath = storeDir.resolve("store").toString();
    TomcatURLStreamHandlerFactory.register();
    final SparkSession spark =
        SparkSession.builder()
            .appName("SqlConceptMapLocalIT")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    final SnomedRf2Importer snomedImporter = new SnomedRf2Importer(spark, storagePath);
    snomedImporter.importFrom(release.toString(), null);
    snomedImporter.importFrom(laterRelease.toString(), null);
    new FhirTerminologyImporter(spark, storagePath).importFrom(resources.toString(), false, null);
    return storagePath;
  }

  /**
   * Appends REPLACED BY, POSSIBLY EQUIVALENT TO and ALTERNATIVE rows to the association reference
   * set file of an extracted base release, beside its SAME AS rows, as the terminology module's
   * implicit concept map test does. The rows are written out of code order, and one POSSIBLY
   * EQUIVALENT TO concept has three targets, one of which is not in the concept dictionary.
   */
  private static void appendAssociationRows(@Nonnull final Path release) {
    final List<String> rows =
        List.of(
            associationRow("340", REPLACED_BY, Rf2Mini.ASSOCIATED_FILLER_2, Rf2Mini.DIABETES),
            associationRow("341", REPLACED_BY, Rf2Mini.DIABETES_INACTIVE, Rf2Mini.TYPE2_DIABETES),
            associationRow("342", POSSIBLY_EQUIVALENT_TO, Rf2Mini.GESTATIONAL_DIABETES, ABSENT),
            associationRow(
                "343",
                POSSIBLY_EQUIVALENT_TO,
                Rf2Mini.GESTATIONAL_DIABETES,
                Rf2Mini.TYPE2_WITH_COMPLICATION),
            associationRow(
                "344",
                POSSIBLY_EQUIVALENT_TO,
                Rf2Mini.GESTATIONAL_DIABETES,
                Rf2Mini.TYPE1_DIABETES),
            associationRow("345", ALTERNATIVE, Rf2Mini.HYPERTENSION, Rf2Mini.DISORDER),
            associationRow("346", ALTERNATIVE, Rf2Mini.DIABETES_INACTIVE, Rf2Mini.DIABETES));
    try (Stream<Path> paths = Files.walk(release)) {
      final Path file =
          paths
              .filter(path -> path.getFileName().toString().startsWith("der2_cRefset_Association"))
              .min(Comparator.naturalOrder())
              .orElseThrow(() -> new IllegalStateException("No association reference set file"));
      final List<String> lines = new ArrayList<>(Files.readAllLines(file));
      lines.addAll(rows);
      Files.write(file, lines);
    } catch (final IOException e) {
      throw new UncheckedIOException("Unable to extend the association reference set", e);
    }
  }

  /**
   * Builds an active association reference set row of the base release, whose member identifier
   * ends in the given hexadecimal suffix.
   */
  @Nonnull
  private static String associationRow(
      @Nonnull final String idSuffix,
      @Nonnull final String refset,
      @Nonnull final String referenced,
      @Nonnull final String target) {
    return String.join(
        "\t",
        "00000000-0000-4000-8000-000000000" + idSuffix,
        "20230601",
        "1",
        Rf2Mini.CORE_MODULE,
        refset,
        referenced,
        target);
  }

  /**
   * Writes the FHIR terminology resources the store imports: the worked example at versions {@code
   * 2026} and {@code 2025}, a copy of version {@code 2026} at a canonical URL with a query, and a
   * value set at another such URL.
   */
  @Nonnull
  private static Path writeResources() {
    final FhirContext context = FhirContext.forR4();
    final IParser parser = context.newJsonParser();
    final ConceptMap version2026 = SqlConceptMapTestConfiguration.workedExample(context);
    final ConceptMap version2025 = SqlConceptMapTestConfiguration.workedExample(context);
    version2025.setId("sct-to-icd10-2025");
    version2025.setVersion("2025");
    version2025.getGroupFirstRep().getElement().stream()
        .filter(element -> DIABETES_MELLITUS.equals(element.getCode()))
        .findFirst()
        .orElseThrow()
        .getTargetFirstRep()
        .setCode(EDITED_2025_TARGET);
    final ConceptMap withQuery = SqlConceptMapTestConfiguration.workedExample(context);
    withQuery.setId("sct-to-icd10-au");
    withQuery.setUrl(CONCEPT_MAP_WITH_QUERY_URL);
    final ValueSet valueSet = new ValueSet();
    valueSet.setId("x-au");
    valueSet.setUrl(VALUE_SET_WITH_QUERY_URL);
    valueSet.setVersion("2026");
    valueSet.setStatus(PublicationStatus.ACTIVE);
    final ValueSet.ConceptSetComponent include = valueSet.getCompose().addInclude();
    include.setSystem(Rf2Mini.SNOMED_URI);
    include.addConcept().setCode(Rf2Mini.DIABETES);
    include.addConcept().setCode(Rf2Mini.TYPE1_DIABETES);
    try {
      final Path directory = Files.createDirectories(storeDir.resolve("resources"));
      write(parser, directory.resolve("sct-to-icd10-2026.json"), version2026);
      write(parser, directory.resolve("sct-to-icd10-2025.json"), version2025);
      write(parser, directory.resolve("sct-to-icd10-au.json"), withQuery);
      write(parser, directory.resolve("x-au.json"), valueSet);
      return directory;
    } catch (final IOException e) {
      throw new UncheckedIOException("Unable to write the terminology resources", e);
    }
  }

  /** Writes one resource as JSON to the given file. */
  private static void write(
      @Nonnull final IParser parser, @Nonnull final Path file, @Nonnull final Resource resource)
      throws IOException {
    Files.writeString(file, parser.encodeResourceToString(resource));
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
  // Scenario 4: the same rows as SERVER mode.
  // -------------------------------------------------------------------------

  @Test
  void leftJoinTranslatesEachConditionAsInServerMode() {
    final String body = postOk(parametersJson(leftJoinQuery(SCT_TO_ICD10_URL + "|2026")));

    assertThat(rows(body))
        .extracting(
            row ->
                row.get("patient_id")
                    + "/"
                    + row.get("code")
                    + "/"
                    + row.get("target_code")
                    + "/"
                    + row.get("relationship"))
        .containsExactly(
            "Patient/p1/" + MYOCARDIAL_INFARCTION + "/I21/equivalent",
            "Patient/p2/" + ACUTE_MYOCARDIAL_INFARCTION + "/null/null",
            "Patient/p3/" + DIABETES_MELLITUS + "/E14/source-is-broader-than-target",
            "Patient/p4/" + FIT_AND_WELL + "/null/null");
  }

  @Test
  void selectingTheRelationReturnsTheWorkedExampleRowsAsInServerMode() {
    assertWorkedExampleRows(SCT_TO_ICD10_URL + "|2026");
  }

  // -------------------------------------------------------------------------
  // Scenarios 5 and 6: pinned and unpinned version selection.
  // -------------------------------------------------------------------------

  @Test
  void pinnedReferenceReadsThatVersion() {
    final String body = postOk(parametersJson(selectTargets(SCT_TO_ICD10_URL + "|2025")));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .containsExactly(
            FIT_AND_WELL + "/null",
            MYOCARDIAL_INFARCTION + "/I21",
            DIABETES_MELLITUS + "/" + EDITED_2025_TARGET);
  }

  @Test
  void unpinnedReferenceReadsTheLatestVersion() {
    final String body = postOk(parametersJson(selectTargets(SCT_TO_ICD10_URL)));

    assertThat(rowsOf(body, "source_code", "target_code"))
        .containsExactly(
            FIT_AND_WELL + "/null", MYOCARDIAL_INFARCTION + "/I21", DIABETES_MELLITUS + "/E14");
  }

  // -------------------------------------------------------------------------
  // Scenario 14: a pinned artefact imported at a canonical URL that carries a query.
  // -------------------------------------------------------------------------

  @Test
  void pinnedConceptMapAtAUrlWithAQueryResolves() {
    assertWorkedExampleRows(CONCEPT_MAP_WITH_QUERY_URL + "|2026");
  }

  @Test
  void pinnedValueSetAtAUrlWithAQueryResolvesAsAValueSet() {
    final Library library =
        sqlQueryLibrary(
            "SELECT system, code FROM x ORDER BY code",
            Map.of("x", VALUE_SET_WITH_QUERY_URL + "|2026"));

    final String body = postOk(parametersJson(library));

    assertThat(rowsOf(body, "system", "code"))
        .containsExactly(
            Rf2Mini.SNOMED_URI + "/" + Rf2Mini.DIABETES,
            Rf2Mini.SNOMED_URI + "/" + Rf2Mini.TYPE1_DIABETES);
  }

  // -------------------------------------------------------------------------
  // US4 scenarios 1-3: the four SNOMED CT implicit concept maps.
  // -------------------------------------------------------------------------

  @Test
  void sameAsImplicitMapHoldsOneEquivalentRowPerAssociationRow() {
    assertThat(implicitRows(Rf2Mini.VERSION_20230601, SAME_AS))
        .containsExactly(
            row(Rf2Mini.DIABETES_INACTIVE, "Diabetes", Rf2Mini.TYPE2_DIABETES, T2DM, EQUIVALENT),
            row(
                Rf2Mini.ASSOCIATED_FILLER_1,
                "Mini diabetes subtype 86",
                Rf2Mini.TYPE2_DIABETES,
                T2DM,
                EQUIVALENT),
            row(
                Rf2Mini.ASSOCIATED_FILLER_2,
                "Mini other disorder 36",
                Rf2Mini.TYPE2_DIABETES,
                T2DM,
                EQUIVALENT),
            row(
                Rf2Mini.ASSOCIATED_FILLER_3,
                "Mini other disorder 56",
                Rf2Mini.TYPE2_DIABETES,
                T2DM,
                EQUIVALENT));
  }

  @Test
  void replacedByImplicitMapHoldsOneEquivalentRowPerAssociationRow() {
    assertThat(implicitRows(Rf2Mini.VERSION_20230601, REPLACED_BY))
        .containsExactly(
            row(Rf2Mini.DIABETES_INACTIVE, "Diabetes", Rf2Mini.TYPE2_DIABETES, T2DM, EQUIVALENT),
            FIXTURE_REPLACED_BY_ROW,
            row(
                Rf2Mini.ASSOCIATED_FILLER_2,
                "Mini other disorder 36",
                Rf2Mini.DIABETES,
                "Diabetes mellitus",
                EQUIVALENT));
  }

  @Test
  void possiblyEquivalentToImplicitMapHoldsARowForEachOfThreeTargets() {
    // One concept has three targets, one of which is not in the dictionary and has no display. The
    // fixture's own source concept has two more.
    final String gestational = "Gestational diabetes mellitus";
    final String fixtureSource = "Mini diabetes subtype 2";
    assertThat(implicitRows(Rf2Mini.VERSION_20230601, POSSIBLY_EQUIVALENT_TO))
        .containsExactly(
            row(
                Rf2Mini.GESTATIONAL_DIABETES,
                gestational,
                Rf2Mini.TYPE1_DIABETES,
                T1DM,
                RELATED_TO),
            row(
                Rf2Mini.GESTATIONAL_DIABETES,
                gestational,
                Rf2Mini.TYPE2_WITH_COMPLICATION,
                "Type 2 diabetes mellitus with complication",
                RELATED_TO),
            row(Rf2Mini.GESTATIONAL_DIABETES, gestational, ABSENT, null, RELATED_TO),
            row(
                Rf2Mini.POSSIBLY_EQUIVALENT_TO_SOURCE,
                fixtureSource,
                Rf2Mini.TYPE1_DIABETES,
                T1DM,
                RELATED_TO),
            row(
                Rf2Mini.POSSIBLY_EQUIVALENT_TO_SOURCE,
                fixtureSource,
                Rf2Mini.TYPE2_DIABETES,
                T2DM,
                RELATED_TO));
  }

  @Test
  void alternativeImplicitMapHoldsOneRelatedToRowPerAssociationRow() {
    assertThat(implicitRows(Rf2Mini.VERSION_20230601, ALTERNATIVE))
        .containsExactly(
            row(
                Rf2Mini.HYPERTENSION,
                "Hypertensive disorder",
                Rf2Mini.DISORDER,
                "Mini disorder",
                RELATED_TO),
            row(
                Rf2Mini.DIABETES_INACTIVE,
                "Diabetes",
                Rf2Mini.DIABETES,
                "Diabetes mellitus",
                RELATED_TO),
            row(
                Rf2Mini.ALTERNATIVE_SOURCE,
                "Mini diabetes subtype 3",
                Rf2Mini.GESTATIONAL_DIABETES,
                "Gestational diabetes mellitus",
                RELATED_TO));
  }

  // -------------------------------------------------------------------------
  // US4 scenario 4: the version comes from the base of the URL.
  // -------------------------------------------------------------------------

  @Test
  void editionVersionBaseSelectsThatVersion() {
    // Only the base release gains the appended REPLACED BY rows; the later release holds the
    // fixture's single row.
    assertThat(implicitRows(Rf2Mini.VERSION_20230601, REPLACED_BY)).hasSize(3);
    assertThat(implicitRows(Rf2Mini.VERSION_20240601, REPLACED_BY))
        .containsExactly(FIXTURE_REPLACED_BY_ROW);
  }

  @Test
  void bareBaseSelectsTheDefaultVersion() {
    // The later release is the store's default SNOMED CT version: its rows carry its version URI
    // (which implicitRows asserts), and it holds only the fixture's REPLACED BY row.
    assertThat(implicitRows(Rf2Mini.SNOMED_URI, SAME_AS)).hasSize(4);
    assertThat(implicitRows(Rf2Mini.SNOMED_URI, REPLACED_BY))
        .containsExactly(FIXTURE_REPLACED_BY_ROW);
  }

  // -------------------------------------------------------------------------
  // US4 scenario 5: a reference set outside the four.
  // -------------------------------------------------------------------------

  @Test
  void implicitMapOverAnotherReferenceSetIsNotFound() {
    final String url = implicitUrl(Rf2Mini.SNOMED_URI, MOVED_FROM);

    final String body =
        postExpectStatus(parametersJson(sqlQueryLibrary("SELECT 1", Map.of("moved", url))), 404);

    assertThat(body)
        .contains(
            "Failed to resolve the dependency for label 'moved' with reference '"
                + url
                + "': no ViewDefinition, SQLView, external table, concept map or value set matches"
                + " that canonical URL")
        .doesNotContain("local terminology mode");
  }

  // -------------------------------------------------------------------------
  // US5 scenario 9: a pinned SNOMED CT implicit concept map URL.
  // -------------------------------------------------------------------------

  @Test
  void pinnedImplicitMapIsA422StatingThatTheVersionCannotBeDetermined() {
    final String url = implicitUrl(Rf2Mini.SNOMED_URI, SAME_AS);

    final String body =
        postExpectStatus(
            parametersJson(
                sqlQueryLibrary("SELECT * FROM same_as", Map.of("same_as", url + "|20230601"))),
            422);

    final OperationOutcome outcome = jsonParser.parseResource(OperationOutcome.class, body);
    assertThat(outcome.getIssue())
        .extracting(OperationOutcomeIssueComponent::getDiagnostics)
        .containsExactly(
            "The mappings of the concept map for label 'same_as' (canonical URL '"
                + url
                + "') could not be determined: cannot determine which version to use: an implicit"
                + " concept map URL carries its version in its base");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Reads the relation of the implicit concept map over the given reference set at the given SNOMED
   * CT base, asserting that every row has SNOMED CT as both systems and the resolved version URI as
   * both versions, and returns each row as {@code
   * <source_code>/<source_display>/<target_code>/<target_display>/<relationship>}, ordered by
   * source code and then target code.
   */
  @Nonnull
  private List<String> implicitRows(@Nonnull final String base, @Nonnull final String refset) {
    // A bare base resolves to the store's default version, the later release.
    final String version = Rf2Mini.SNOMED_URI.equals(base) ? Rf2Mini.VERSION_20240601 : base;
    final List<Map<String, Object>> rows =
        rows(
            postOk(
                parametersJson(
                    sqlQueryLibrary(
                        "SELECT * FROM implicit_map ORDER BY source_code, target_code",
                        Map.of("implicit_map", implicitUrl(base, refset))))));
    for (final Map<String, Object> row : rows) {
      assertThat(row)
          .containsEntry("source_system", Rf2Mini.SNOMED_URI)
          .containsEntry("target_system", Rf2Mini.SNOMED_URI)
          .containsEntry("source_version", version)
          .containsEntry("target_version", version);
    }
    return rows.stream()
        .map(
            row ->
                row(
                    (String) row.get("source_code"),
                    (String) row.get("source_display"),
                    (String) row.get("target_code"),
                    (String) row.get("target_display"),
                    (String) row.get("relationship")))
        .toList();
  }

  /** Formats one implicit map row as {@link #implicitRows} returns it, a null as {@code null}. */
  @Nonnull
  private static String row(
      @Nullable final String sourceCode,
      @Nullable final String sourceDisplay,
      @Nullable final String targetCode,
      @Nullable final String targetDisplay,
      @Nullable final String relationship) {
    return sourceCode
        + "/"
        + sourceDisplay
        + "/"
        + targetCode
        + "/"
        + targetDisplay
        + "/"
        + relationship;
  }

  /** Builds the implicit concept map URL over a reference set at a SNOMED CT base. */
  @Nonnull
  private static String implicitUrl(@Nonnull final String base, @Nonnull final String refset) {
    return base + "?fhir_cm=" + refset;
  }

  /** Asserts that the relation of the given reference holds the worked example's three rows. */
  private void assertWorkedExampleRows(@Nonnull final String canonical) {
    final String body =
        postOk(
            parametersJson(
                sqlQueryLibrary(
                    "SELECT source_system, source_version, source_code, source_display,"
                        + " target_system, target_version, target_code, target_display,"
                        + " relationship FROM sct_to_icd10 ORDER BY source_code",
                    Map.of("sct_to_icd10", canonical))));

    final List<Map<String, Object>> rows = rows(body);
    assertThat(rows).hasSize(3);
    for (final Map<String, Object> row : rows) {
      assertThat(row)
          .containsEntry("source_system", SNOMED)
          .containsEntry("target_system", ICD10)
          .containsEntry("target_version", "2019");
      assertThat(row.get("source_version")).isNull();
    }
    assertThat(rows.get(0)).containsEntry("source_code", FIT_AND_WELL);
    assertThat(rows.get(0).get("target_code")).isNull();
    assertThat(rows.get(0).get("target_display")).isNull();
    assertThat(rows.get(0).get("relationship")).isNull();
    assertThat(rows.get(1))
        .containsEntry("source_code", MYOCARDIAL_INFARCTION)
        .containsEntry("source_display", "Myocardial infarction")
        .containsEntry("target_code", "I21")
        .containsEntry("target_display", "Acute myocardial infarction")
        .containsEntry("relationship", "equivalent");
    assertThat(rows.get(2))
        .containsEntry("source_code", DIABETES_MELLITUS)
        .containsEntry("target_code", "E14")
        .containsEntry("relationship", "source-is-broader-than-target");
  }

  /** Builds the SQLQuery left-joining the Condition view to the given concept map. */
  @Nonnull
  Library leftJoinQuery(@Nonnull final String conceptMapCanonical) {
    final Map<String, String> dependencies = new LinkedHashMap<>();
    dependencies.put("conditions", SqlConceptMapTestConfiguration.CONDITION_VIEW_URL);
    dependencies.put("sct_to_icd10", conceptMapCanonical);
    return sqlQueryLibrary(
        "SELECT conditions.patient_id, conditions.code, sct_to_icd10.target_code,"
            + " sct_to_icd10.relationship"
            + " FROM conditions"
            + " LEFT JOIN sct_to_icd10"
            + " ON sct_to_icd10.source_system = conditions.system"
            + " AND sct_to_icd10.source_code = conditions.code"
            + " AND (sct_to_icd10.relationship IS NULL"
            + " OR sct_to_icd10.relationship <> 'not-related-to')"
            + " ORDER BY conditions.id",
        dependencies);
  }

  /** Builds a SQLQuery selecting each source and target code of the given concept map. */
  @Nonnull
  Library selectTargets(@Nonnull final String conceptMapCanonical) {
    return sqlQueryLibrary(
        "SELECT source_code, target_code FROM sct_to_icd10 ORDER BY source_code",
        Map.of("sct_to_icd10", conceptMapCanonical));
  }

  /** Parses each NDJSON row of a response body into a map, in response order; none if blank. */
  @Nonnull
  @SuppressWarnings("unchecked")
  List<Map<String, Object>> rows(@Nonnull final String body) {
    if (body.isBlank()) {
      return List.of();
    }
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

  /** Encodes a resource as the generic JSON map the Gson-built request bodies carry. */
  @Nonnull
  @SuppressWarnings("unchecked")
  Map<String, Object> resourceMap(@Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    return gson.fromJson(jsonParser.encodeResourceToString(resource), Map.class);
  }

  /**
   * Wraps the Library as the {@code subjectResource} of a {@code $sql-run} Parameters body, along
   * with the NDJSON format.
   */
  @Nonnull
  String parametersJson(@Nonnull final Library library) {
    final Map<String, Object> parameters =
        parameters(
            resourcePart("subjectResource", resourceMap(library)),
            simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode()));
    return gson.toJson(parameters);
  }
}

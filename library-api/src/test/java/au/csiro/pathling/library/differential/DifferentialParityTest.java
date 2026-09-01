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

package au.csiro.pathling.library.differential;

import static au.csiro.pathling.library.TerminologyHelpers.SNOMED_URI;
import static au.csiro.pathling.library.TerminologyHelpers.toCoding;
import static au.csiro.pathling.sql.Terminology.designation;
import static au.csiro.pathling.sql.Terminology.display;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.sql.Terminology.property_of;
import static au.csiro.pathling.sql.Terminology.subsumed_by;
import static au.csiro.pathling.sql.Terminology.subsumes;
import static au.csiro.pathling.sql.Terminology.translate;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Differential parity suite for local terminology mode (success criterion SC-001).
 *
 * <p>The suite evaluates a corpus of queries covering all seven terminology functions - {@code
 * member_of} (SNOMED implicit value sets, the supported ECL subset, and VCL URLs), {@code
 * subsumes}, {@code subsumed_by}, {@code display}, {@code designation}, {@code property_of}, and
 * {@code translate} (a SNOMED implicit concept map) - against both the local index service and a
 * reference terminology server loaded with the same SNOMED CT edition, and asserts that every
 * result matches.
 *
 * <p>The suite is tagged {@code differential} and is excluded from the default build by an
 * assumption that skips it unless the reference server and store are configured. Run it on demand:
 *
 * <pre>{@code
 * cd library-api
 * mvn test -Dgroups=differential \
 *   -Dpathling.test.txServerUrl=http://tx.example.org/fhir \
 *   -Dpathling.test.local.storagePath=/data/tx-store
 * }</pre>
 *
 * <p>If {@code pathling.test.rf2Path} is also supplied and the store is empty, the suite imports
 * that RF2 archive into the store first (timing the import), so a single command can provision and
 * validate the store. See the {@code README.md} alongside this class for the full procedure.
 *
 * @author John Grimes
 */
@Tag("differential")
class DifferentialParityTest {

  private static final Logger log = LoggerFactory.getLogger(DifferentialParityTest.class);

  /** System property naming the reference terminology server base URL. */
  private static final String SERVER_URL_PROPERTY = "pathling.test.txServerUrl";

  /** System property naming the local terminology store path. */
  private static final String STORAGE_PATH_PROPERTY = "pathling.test.local.storagePath";

  /** System property naming an RF2 archive to import when the store is empty. */
  private static final String RF2_PATH_PROPERTY = "pathling.test.rf2Path";

  /**
   * Optional system property naming a SNOMED CT edition/version URI (for example {@code
   * http://snomed.info/sct/32506021000036107/version/20260430}). When set, every corpus reference
   * is pinned to this version on both sides, so a reference server holding several SNOMED releases
   * can be compared against a store holding exactly one.
   */
  private static final String SNOMED_VERSION_PROPERTY = "pathling.test.snomedVersion";

  // SNOMED CT International core concepts, present in any derived edition.
  private static final String DIABETES = "73211009"; // Diabetes mellitus.
  private static final String TYPE_1_DIABETES = "46635009";
  private static final String TYPE_2_DIABETES = "44054006";
  private static final String HYPERTENSION = "38341003";
  private static final String ASTHMA = "195967001";
  private static final String SPECIMEN_FLUID = "122566000"; // Member of specimen-type refset.
  private static final String SPECIMEN_GALLSTONE = "258492001"; // Member of specimen-type refset.

  private static final String SPECIMEN_TYPE_REFSET = "4021000036102";
  private static final String SAME_AS_REFSET = "900000000000527005";
  private static final String SYNONYM_USE = "900000000000013009"; // Synonym description type.

  // Inactive concepts with a SAME AS association, for the translate corpus.
  private static final String INACTIVE_SOURCE_A = "146181009"; // Orbital venography.
  private static final String INACTIVE_SOURCE_B = "255182002"; // Syringoadenoma.

  private static Map<String, Object> localResults;
  private static Map<String, Object> remoteResults;

  @BeforeAll
  static void evaluateCorpus() {
    final String serverUrl = System.getProperty(SERVER_URL_PROPERTY);
    final String storagePath = System.getProperty(STORAGE_PATH_PROPERTY);
    assumeTrue(
        serverUrl != null && !serverUrl.isBlank() && storagePath != null && !storagePath.isBlank(),
        "Set -D"
            + SERVER_URL_PROPERTY
            + " and -D"
            + STORAGE_PATH_PROPERTY
            + " to run the differential parity suite.");

    final SparkSession spark = TestHelpers.spark();
    provisionStoreIfEmpty(spark, storagePath);

    final Map<String, Column> corpus = corpus();
    log.info("Evaluating {} differential queries against local mode.", corpus.size());
    localResults = evaluate(spark, localConfiguration(storagePath), corpus);
    log.info("Evaluating {} differential queries against {}.", corpus.size(), serverUrl);
    remoteResults = evaluate(spark, remoteConfiguration(serverUrl), corpus);
  }

  @AfterAll
  static void tearDown() {
    LocalTerminologyServiceFactory.reset();
  }

  /**
   * Imports the configured RF2 archive into the store when the store is empty and an archive path
   * is supplied, timing the import so its duration can be recorded for the scale validation (T053).
   */
  private static void provisionStoreIfEmpty(
      @Nonnull final SparkSession spark, @Nonnull final String storagePath) {
    final String rf2Path = System.getProperty(RF2_PATH_PROPERTY);
    if (rf2Path == null || rf2Path.isBlank() || storeHasContent(storagePath)) {
      return;
    }
    log.info("Store at {} is empty; importing SNOMED CT from {}.", storagePath, rf2Path);
    final long start = System.nanoTime();
    PathlingContext.builder(spark).build().importSnomed(rf2Path, storagePath, null);
    final long seconds = (System.nanoTime() - start) / 1_000_000_000L;
    log.info("Imported SNOMED CT into {} in {} seconds.", storagePath, seconds);
  }

  private static boolean storeHasContent(@Nonnull final String storagePath) {
    final Path path = Path.of(storagePath);
    if (!Files.isDirectory(path)) {
      return false;
    }
    try (final Stream<Path> entries = Files.list(path)) {
      return entries.findAny().isPresent();
    } catch (final Exception e) {
      return false;
    }
  }

  @Test
  void memberOfMatches() {
    assertParity("member_of");
  }

  @Test
  void subsumesMatches() {
    assertParity("subsumes");
  }

  @Test
  void subsumedByMatches() {
    assertParity("subsumed_by");
  }

  @Test
  void displayMatches() {
    assertParity("display");
  }

  @Test
  void designationMatches() {
    assertParity("designation");
  }

  @Test
  void propertyOfMatches() {
    assertParity("property_of");
  }

  @Test
  void translateMatches() {
    assertParity("translate");
  }

  /**
   * Asserts that every corpus query in the given category returns identical results in local and
   * remote mode. Fails with the full list of mismatches so an entire category can be triaged from a
   * single run.
   *
   * @param category the query id prefix identifying the function under test
   */
  private void assertParity(@Nonnull final String category) {
    final String prefix = category + "::";
    final List<String> ids =
        localResults.keySet().stream().filter(id -> id.startsWith(prefix)).sorted().toList();
    assertTrue(!ids.isEmpty(), "No corpus queries were defined for category: " + category);

    final List<String> mismatches = new ArrayList<>();
    for (final String id : ids) {
      final Object local = localResults.get(id);
      final Object remote = remoteResults.get(id);
      if (!Objects.equals(local, remote)) {
        mismatches.add(id + " -> local=" + local + ", remote=" + remote);
      }
    }
    assertEquals(
        List.of(),
        mismatches,
        () ->
            category
                + " parity mismatches ("
                + mismatches.size()
                + "):\n"
                + String.join("\n", mismatches));
  }

  /**
   * Builds the differential corpus as an ordered map of query id to the column expression that
   * computes its result. The same expressions are evaluated under both backends, so the assertion
   * compares two implementations of identical semantics rather than against hardcoded answers.
   *
   * @return an ordered map of query id to column expression
   */
  @Nonnull
  private static Map<String, Column> corpus() {
    // When pathling.test.snomedVersion is set, all references are pinned to that release on both
    // sides; otherwise references are left unversioned and each side resolves them against its
    // single loaded SNOMED release.
    final String system = System.getProperty(SNOMED_VERSION_PROPERTY, SNOMED_URI);
    final String eclDiabetes = system + "?fhir_vs=ecl/" + encode("<< " + DIABETES);
    final String isaDiabetes = system + "?fhir_vs=isa/" + DIABETES;
    final String refsetSpecimen = system + "?fhir_vs=refset/" + SPECIMEN_TYPE_REFSET;
    final String vclDiabetes =
        "http://fhir.org/VCL?v1=" + encode("(" + system + ")concept << " + DIABETES);
    final String sameAsMap = system + "?fhir_cm=" + SAME_AS_REFSET;

    final List<String> membershipSubjects =
        List.of(DIABETES, TYPE_1_DIABETES, TYPE_2_DIABETES, HYPERTENSION, ASTHMA, SPECIMEN_FLUID);

    final Map<String, Column> corpus = new LinkedHashMap<>();

    // member_of across the SNOMED implicit forms, the ECL subset, and VCL.
    addMembership(corpus, "ecl", eclDiabetes, membershipSubjects);
    addMembership(corpus, "isa", isaDiabetes, membershipSubjects);
    addMembership(corpus, "vcl", vclDiabetes, membershipSubjects);
    addMembership(
        corpus, "refset", refsetSpecimen, List.of(SPECIMEN_FLUID, SPECIMEN_GALLSTONE, DIABETES));

    // subsumes / subsumed_by across the hierarchy.
    corpus.put("subsumes::diabetes_type2", subsumes(snomed(DIABETES), snomed(TYPE_2_DIABETES)));
    corpus.put("subsumes::type2_diabetes", subsumes(snomed(TYPE_2_DIABETES), snomed(DIABETES)));
    corpus.put("subsumes::diabetes_hypertension", subsumes(snomed(DIABETES), snomed(HYPERTENSION)));
    corpus.put(
        "subsumed_by::type2_diabetes", subsumed_by(snomed(TYPE_2_DIABETES), snomed(DIABETES)));
    corpus.put(
        "subsumed_by::diabetes_type2", subsumed_by(snomed(DIABETES), snomed(TYPE_2_DIABETES)));

    // display for a spread of concepts.
    for (final String code : List.of(DIABETES, TYPE_2_DIABETES, HYPERTENSION)) {
      corpus.put("display::" + code, display(snomed(code)));
    }

    // designation (synonyms) for a stable concept.
    final Coding synonymUse = new Coding().setSystem(SNOMED_URI).setCode(SYNONYM_USE);
    corpus.put("designation::" + DIABETES, designation(snomed(DIABETES), synonymUse, "en"));
    corpus.put(
        "designation::" + TYPE_2_DIABETES, designation(snomed(TYPE_2_DIABETES), synonymUse, "en"));

    // property_of for standard SNOMED properties.
    corpus.put(
        "property_of::parent::" + TYPE_2_DIABETES,
        property_of(snomed(TYPE_2_DIABETES), "parent", FHIRDefinedType.CODE));
    corpus.put(
        "property_of::parent::" + TYPE_1_DIABETES,
        property_of(snomed(TYPE_1_DIABETES), "parent", FHIRDefinedType.CODE));
    corpus.put(
        "property_of::inactive::" + DIABETES,
        property_of(snomed(DIABETES), "inactive", FHIRDefinedType.BOOLEAN));

    // translate via the SNOMED implicit SAME AS association concept map.
    corpus.put(
        "translate::" + INACTIVE_SOURCE_A,
        translate(snomed(INACTIVE_SOURCE_A), sameAsMap, false, null));
    corpus.put(
        "translate::" + INACTIVE_SOURCE_B,
        translate(snomed(INACTIVE_SOURCE_B), sameAsMap, false, null));

    return corpus;
  }

  private static void addMembership(
      @Nonnull final Map<String, Column> corpus,
      @Nonnull final String label,
      @Nonnull final String valueSetUrl,
      @Nonnull final List<String> subjects) {
    for (final String code : subjects) {
      corpus.put("member_of::" + label + "::" + code, member_of(snomed(code), valueSetUrl));
    }
  }

  /**
   * Evaluates the whole corpus in a single row under the given terminology configuration and
   * returns a normalised map of query id to result. Array results (designations, properties,
   * translation targets) are reduced to sorted lists of strings so that set-valued results compare
   * equal regardless of order.
   */
  @Nonnull
  private static Map<String, Object> evaluate(
      @Nonnull final SparkSession spark,
      @Nonnull final TerminologyConfiguration configuration,
      @Nonnull final Map<String, Column> corpus) {
    LocalTerminologyServiceFactory.reset();
    PathlingContext.builder(spark).terminologyConfiguration(configuration).build();

    final Dataset<Row> seed =
        spark.createDataFrame(
            List.of(RowFactory.create("row")),
            new StructType().add("id", DataTypes.StringType, true));

    final List<String> ids = new ArrayList<>(corpus.keySet());
    final Column[] columns = new Column[ids.size()];
    for (int i = 0; i < ids.size(); i++) {
      columns[i] = corpus.get(ids.get(i)).alias("c" + i);
    }
    final Row row = seed.select(columns).collectAsList().get(0);
    final StructType schema = row.schema();

    final Map<String, Object> results = new LinkedHashMap<>();
    for (int i = 0; i < ids.size(); i++) {
      results.put(ids.get(i), normalise(row, i, schema.fields()[i]));
    }
    return results;
  }

  /**
   * Normalises a single result cell. Scalars (booleans, strings) are returned as-is; arrays are
   * flattened to a sorted list of string representations, extracting the {@code code} field from
   * Coding structs (as returned by {@code translate}).
   */
  private static Object normalise(
      @Nonnull final Row row, final int index, @Nonnull final StructField field) {
    if (row.isNullAt(index)) {
      return null;
    }
    if (field.dataType() instanceof ArrayType) {
      final List<?> values = row.getList(index);
      final List<String> normalised = new ArrayList<>();
      for (final Object value : values) {
        if (value instanceof Row coding) {
          normalised.add(String.valueOf(coding.getAs("code")));
        } else {
          normalised.add(String.valueOf(value));
        }
      }
      Collections.sort(normalised);
      return normalised;
    }
    return row.get(index);
  }

  @Nonnull
  private static Column snomed(@Nonnull final String code) {
    return toCoding(lit(code), SNOMED_URI, System.getProperty(SNOMED_VERSION_PROPERTY));
  }

  @Nonnull
  private static TerminologyConfiguration localConfiguration(@Nonnull final String storagePath) {
    return TerminologyConfiguration.builder()
        .mode(TerminologyMode.LOCAL)
        .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
        .build();
  }

  @Nonnull
  private static TerminologyConfiguration remoteConfiguration(@Nonnull final String serverUrl) {
    return TerminologyConfiguration.builder()
        .mode(TerminologyMode.SERVER)
        .serverUrl(serverUrl)
        .build();
  }

  @Nonnull
  private static String encode(@Nonnull final String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }
}

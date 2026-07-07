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

package au.csiro.pathling.library;

import static au.csiro.pathling.library.TerminologyHelpers.SNOMED_URI;
import static au.csiro.pathling.library.TerminologyHelpers.toCoding;
import static au.csiro.pathling.sql.Terminology.designation;
import static au.csiro.pathling.sql.Terminology.display;
import static au.csiro.pathling.sql.Terminology.property_of;
import static au.csiro.pathling.sql.Terminology.subsumed_by;
import static au.csiro.pathling.sql.Terminology.subsumes;
import static au.csiro.pathling.sql.Terminology.translate;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of the SNOMED terminology functions in local mode: {@code subsumes}, {@code
 * subsumed_by}, {@code display}, {@code property_of}, and {@code designation} are evaluated as UDFs
 * over the rf2-mini store imported through the public library API, and {@code translate} returns
 * the unknown-content fallback for a value set with no imported concept map (quickstart scenario 4
 * at the Java level).
 *
 * @author John Grimes
 */
class LocalTerminologyFunctionsTest {

  private static final String SYNONYM = "900000000000013009";

  private static SparkSession spark;
  private static String store;

  @BeforeAll
  static void setUp(@TempDir final Path storeDir) {
    spark = TestHelpers.spark();
    store = storeDir.resolve("store").toString();
    PathlingContext.builder(spark).build().importSnomed(fixturePath(), store, null);
  }

  @AfterAll
  static void tearDown() {
    LocalTerminologyServiceFactory.reset();
  }

  @BeforeEach
  void createLocalContext() {
    LocalTerminologyServiceFactory.reset();
    PathlingContext.builder(spark)
        .terminologyConfiguration(
            TerminologyConfiguration.builder()
                .mode(TerminologyMode.LOCAL)
                .local(LocalTerminologyConfiguration.builder().storagePath(store).build())
                .build())
        .build();
  }

  private static String fixturePath() {
    return Path.of(
            "..", "terminology", "src", "test", "resources", "rf2-mini", "international-20230601")
        .toAbsolutePath()
        .normalize()
        .toString();
  }

  private static Column snomed(final String code) {
    return toCoding(lit(code), SNOMED_URI, null);
  }

  @Nonnull
  private static Row evaluate(final Column result) {
    final Dataset<Row> df =
        spark.createDataFrame(
            List.of(RowFactory.create("row")),
            new StructType().add("id", DataTypes.StringType, true));
    return df.select(result.alias("result")).collectAsList().get(0);
  }

  @Test
  void subsumesReflectsHierarchy() {
    assertEquals(
        Boolean.TRUE,
        evaluate(subsumes(snomed(Rf2Mini.DIABETES), snomed(Rf2Mini.TYPE2_DIABETES))).getBoolean(0));
    assertEquals(
        Boolean.FALSE,
        evaluate(subsumes(snomed(Rf2Mini.TYPE2_DIABETES), snomed(Rf2Mini.DIABETES))).getBoolean(0));
  }

  @Test
  void subsumedByReflectsHierarchy() {
    assertEquals(
        Boolean.TRUE,
        evaluate(subsumed_by(snomed(Rf2Mini.TYPE2_DIABETES), snomed(Rf2Mini.DIABETES)))
            .getBoolean(0));
  }

  @Test
  void displayReturnsPreferredTerm() {
    assertEquals("Diabetes mellitus", evaluate(display(snomed(Rf2Mini.DIABETES))).getString(0));
  }

  @Test
  void propertyOfReturnsParentCode() {
    final List<String> parents =
        evaluate(property_of(snomed(Rf2Mini.TYPE2_DIABETES), "parent", FHIRDefinedType.CODE))
            .getList(0);
    assertEquals(List.of(Rf2Mini.DIABETES), parents);
  }

  @Test
  void designationReturnsSynonyms() {
    final Coding synonymUse = new Coding().setSystem(SNOMED_URI).setCode(SYNONYM);
    final List<String> designations =
        evaluate(designation(snomed(Rf2Mini.DIABETES), synonymUse, "en")).getList(0);
    assertTrue(designations.contains("Diabetes mellitus"));
  }

  @Test
  void translateReturnsEmptyForUnknownConceptMap() {
    // No concept map has been imported, so translation yields the unknown-content fallback.
    final List<Row> matches =
        evaluate(
                translate(
                    snomed(Rf2Mini.DIABETES),
                    "http://snomed.info/sct?fhir_cm=900000000000527005",
                    false,
                    null))
            .getList(0);
    assertTrue(matches.isEmpty());
  }
}

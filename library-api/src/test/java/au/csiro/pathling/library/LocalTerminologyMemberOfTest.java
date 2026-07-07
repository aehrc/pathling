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
import static au.csiro.pathling.library.TerminologyHelpers.toEclValueSet;
import static au.csiro.pathling.sql.Terminology.member_of;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import au.csiro.pathling.test.Rf2Mini;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of local-mode {@code member_of}: rf2-mini is imported through {@link
 * PathlingContext#importSnomed}, a local-mode context is created over the resulting store, and the
 * {@code member_of} UDF is evaluated over a DataFrame of codings using ECL and SNOMED implicit
 * value set URLs (quickstart scenario 1 at the Java level).
 *
 * @author John Grimes
 */
class LocalTerminologyMemberOfTest {

  private static SparkSession spark;
  private static String store;

  @BeforeAll
  static void setUp(@TempDir final Path storeDir) {
    spark = TestHelpers.spark();
    store = storeDir.resolve("store").toString();
    // Import through the public library API, using a default-mode context.
    PathlingContext.builder(spark).build().importSnomed(fixturePath(), store, null);
  }

  @AfterAll
  static void tearDown() {
    LocalTerminologyServiceFactory.reset();
  }

  /**
   * Resolves the fixture from the sibling terminology module's source tree. The classpath resource
   * is not usable here because it resides inside the terminology test-jar, from which Spark cannot
   * read a directory.
   */
  private static String fixturePath() {
    return Path.of(
            "..", "terminology", "src", "test", "resources", "rf2-mini", "international-20230601")
        .toAbsolutePath()
        .normalize()
        .toString();
  }

  private void createLocalContext() {
    LocalTerminologyServiceFactory.reset();
    PathlingContext.builder(spark)
        .terminologyConfiguration(
            TerminologyConfiguration.builder()
                .mode(TerminologyMode.LOCAL)
                .local(LocalTerminologyConfiguration.builder().storagePath(store).build())
                .build())
        .build();
  }

  /** Evaluates member_of over the fixture codes and returns a map from code to membership. */
  private Map<String, Boolean> membership(final String valueSetUrl, final String... codes) {
    createLocalContext();
    final List<Row> rows = new ArrayList<>();
    for (final String code : codes) {
      rows.add(RowFactory.create(code));
    }
    final Dataset<Row> df =
        spark.createDataFrame(rows, new StructType().add("code", DataTypes.StringType, true));
    final Column coding = toCoding(df.col("code"), SNOMED_URI, null);
    final Dataset<Row> result =
        df.select(df.col("code"), member_of(coding, valueSetUrl).alias("member"));
    final Map<String, Boolean> membership = new HashMap<>();
    for (final Row row : result.collectAsList()) {
      membership.put(row.getString(0), row.get(1) == null ? null : row.getBoolean(1));
    }
    return membership;
  }

  @Test
  void eclValueSetMembership() {
    final Map<String, Boolean> result =
        membership(
            toEclValueSet("<< " + Rf2Mini.DIABETES),
            Rf2Mini.DIABETES,
            Rf2Mini.TYPE1_DIABETES,
            Rf2Mini.TYPE2_WITH_COMPLICATION,
            Rf2Mini.HYPERTENSION);
    assertEquals(Boolean.TRUE, result.get(Rf2Mini.DIABETES));
    assertEquals(Boolean.TRUE, result.get(Rf2Mini.TYPE1_DIABETES));
    assertEquals(Boolean.TRUE, result.get(Rf2Mini.TYPE2_WITH_COMPLICATION));
    assertEquals(Boolean.FALSE, result.get(Rf2Mini.HYPERTENSION));
  }

  @Test
  void isaImplicitValueSetMembership() {
    final Map<String, Boolean> result =
        membership(
            SNOMED_URI + "?fhir_vs=isa/" + Rf2Mini.DIABETES,
            Rf2Mini.TYPE2_DIABETES,
            Rf2Mini.HYPERTENSION);
    assertEquals(Boolean.TRUE, result.get(Rf2Mini.TYPE2_DIABETES));
    assertEquals(Boolean.FALSE, result.get(Rf2Mini.HYPERTENSION));
  }

  @Test
  void refsetImplicitValueSetMembership() {
    final Map<String, Boolean> result =
        membership(
            SNOMED_URI + "?fhir_vs=refset/" + Rf2Mini.SIMPLE_REFSET,
            Rf2Mini.TYPE1_DIABETES,
            Rf2Mini.DIABETES);
    assertEquals(Boolean.TRUE, result.get(Rf2Mini.TYPE1_DIABETES));
    assertEquals(Boolean.FALSE, result.get(Rf2Mini.DIABETES));
  }

  @Test
  void allConceptsExcludesInactive() {
    final Map<String, Boolean> result =
        membership(SNOMED_URI + "?fhir_vs", Rf2Mini.DIABETES, Rf2Mini.DIABETES_INACTIVE);
    assertEquals(Boolean.TRUE, result.get(Rf2Mini.DIABETES));
    assertEquals(Boolean.FALSE, result.get(Rf2Mini.DIABETES_INACTIVE));
  }
}

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

import static au.csiro.pathling.library.TerminologyHelpers.toCoding;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.sql.Terminology.translate;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of FHIR terminology import through the public library API: the animal-species
 * fixtures are imported with {@link PathlingContext#importFhirTerminology}, a local-mode context is
 * created over the resulting store, and {@code member_of} and {@code translate} are evaluated as
 * UDFs (quickstart scenario 2 at the Java level).
 *
 * @author John Grimes
 */
class LocalTerminologyFhirImportTest {

  private static final String SYSTEM = "http://example.org/fhir/CodeSystem/animal-species";
  private static final String CATEGORY = "http://example.org/fhir/CodeSystem/animal-category";
  private static final String MAMMALS = "http://example.org/fhir/ValueSet/mammals-enumerated";
  private static final String CONCEPT_MAP =
      "http://example.org/fhir/ConceptMap/species-to-category";

  private static SparkSession spark;
  private static String store;

  @BeforeAll
  static void setUp(@TempDir final Path storeDir) {
    spark = TestHelpers.spark();
    store = storeDir.resolve("store").toString();
    PathlingContext.builder(spark).build().importFhirTerminology(fixturePath(), store);
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
    return Path.of("..", "terminology", "src", "test", "resources", "fhir-fixtures", "json")
        .toAbsolutePath()
        .normalize()
        .toString();
  }

  private static Column species(final String code) {
    return toCoding(lit(code), SYSTEM, null);
  }

  private static Row evaluate(final Column result) {
    final Dataset<Row> df =
        spark.createDataFrame(
            List.of(RowFactory.create("row")),
            new StructType().add("id", DataTypes.StringType, true));
    return df.select(result.alias("result")).collectAsList().get(0);
  }

  @Test
  void memberOfExplicitValueSet() {
    assertTrue(evaluate(member_of(species("dog"), MAMMALS)).getBoolean(0));
    assertFalse(evaluate(member_of(species("sparrow"), MAMMALS)).getBoolean(0));
  }

  @Test
  void translateThroughImportedConceptMap() {
    // Dog maps to a single category concept through the imported concept map.
    final List<Row> matches =
        evaluate(translate(species("dog"), CONCEPT_MAP, false, null)).getList(0);
    assertEquals(1, matches.size());
    assertEquals(CATEGORY, matches.get(0).getString(matches.get(0).fieldIndex("system")));
  }
}

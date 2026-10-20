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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.terminology.TerminologyRelationException;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of the value set and concept map relations over a local terminology store: the
 * animal-species fixtures are imported, a local-mode context resolves a value set and a concept map
 * by canonical URL, and the relations are joined to in Spark SQL.
 *
 * @author John Grimes
 */
class LocalTerminologyRelationsTest {

  private static final String SPECIES = "http://example.org/fhir/CodeSystem/animal-species";
  private static final String CATEGORY = "http://example.org/fhir/CodeSystem/animal-category";
  private static final String MAMMALS = "http://example.org/fhir/ValueSet/mammals-enumerated";
  private static final String CONCEPT_MAP =
      "http://example.org/fhir/ConceptMap/species-to-category";

  private static SparkSession spark;
  private static PathlingContext pathling;

  @BeforeAll
  static void setUp(@TempDir final Path storeDir) {
    spark = TestHelpers.spark();
    final String store = storeDir.resolve("store").toString();
    PathlingContext.builder(spark).build().importFhirTerminology(fixturePath(), store);
    LocalTerminologyServiceFactory.reset();
    pathling =
        PathlingContext.builder(spark)
            .terminologyConfiguration(
                TerminologyConfiguration.builder()
                    .mode(TerminologyMode.LOCAL)
                    .local(LocalTerminologyConfiguration.builder().storagePath(store).build())
                    .build())
            .build();
  }

  @AfterAll
  static void tearDown() {
    LocalTerminologyServiceFactory.reset();
  }

  private static String fixturePath() {
    return Path.of("..", "terminology", "src", "test", "resources", "fhir-fixtures", "json")
        .toAbsolutePath()
        .normalize()
        .toString();
  }

  @Test
  void valueSetIsExpandedFromTheStore() {
    // The enumerated value set expands to its three members, carrying the code system version and
    // displays recorded in the store.
    final List<Row> rows =
        pathling
            .valueSet(MAMMALS + "|1.0.0")
            .orderBy("code")
            .select("system", "code")
            .collectAsList();

    assertEquals(
        List.of(
            RowFactory.create(SPECIES, "cat"),
            RowFactory.create(SPECIES, "dog"),
            RowFactory.create(SPECIES, "whale")),
        rows);
  }

  @Test
  void conceptMapIsReadFromTheStoreAndTranslatesInSql() {
    // Each species joins to its category through the imported map; the R4 equivalences are
    // reported as R5 relationships.
    pathling.conceptMap(CONCEPT_MAP).createOrReplaceTempView("species_to_category");
    pathling.valueSet(MAMMALS).createOrReplaceTempView("mammals");

    final List<Row> rows =
        spark
            .sql(
                """
                SELECT m.code, c.target_system, c.target_code, c.relationship
                FROM mammals m
                JOIN species_to_category c
                  ON c.source_system = m.system AND c.source_code = m.code
                ORDER BY m.code
                """)
            .collectAsList();

    assertEquals(
        List.of(
            RowFactory.create("cat", CATEGORY, "pet", "equivalent"),
            RowFactory.create("dog", CATEGORY, "pet", "equivalent"),
            RowFactory.create("whale", CATEGORY, "aquatic", "related-to")),
        rows);
  }

  @Test
  void unknownCanonicalsFail() {
    assertThrows(
        TerminologyRelationException.class,
        () -> pathling.valueSet("http://example.org/fhir/ValueSet/unknown"));
    assertThrows(
        TerminologyRelationException.class,
        () -> pathling.conceptMap("http://example.org/fhir/ConceptMap/unknown"));
  }

  @Test
  void simpleMapIsRejectedWithTheReason() {
    // A simple map translates in local mode, so the error says why it cannot be read rather than
    // reporting it as unresolvable.
    final String url = "http://snomed.info/sct?fhir_cm=446608001";

    final TerminologyRelationException error =
        assertThrows(TerminologyRelationException.class, () -> pathling.conceptMap(url));

    assertEquals(
        "Concept map '"
            + url
            + "' could not be resolved: reference set 446608001 is not one of the four association"
            + " reference sets that can be read as a concept map (POSSIBLY EQUIVALENT TO, REPLACED"
            + " BY, SAME AS and ALTERNATIVE); a simple map reference set can be used with"
            + " translate, but not read as a concept map",
        error.getMessage());
  }
}

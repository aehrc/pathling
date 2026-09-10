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

package au.csiro.pathling.terminology.store;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies the staging layout: a fresh temporary directory holds one NDJSON file per staging table,
 * the appended rows read back through the explicit Spark schemas, and the directory is deleted when
 * the staging is closed on both the success and failure paths.
 *
 * @author John Grimes
 */
class CodeSystemStagingTest {

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@org.junit.jupiter.api.io.TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("CodeSystemStagingTest")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void createsATemporaryDirectory() {
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      assertTrue(Files.isDirectory(staging.getDirectory()));
    }
  }

  @Test
  void roundTripsRowsThroughTheExplicitSchemas() {
    final Path directory;
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      directory = staging.getDirectory();
      staging.appendConcept("A", 0, true, false, "Alpha");
      staging.appendConcept("B", 1, false, false, "Beta");
      staging.appendDescription(0, "AlphaSyn", "en", "synonym", "http://use");
      staging.appendProperty(1, "weight", "integer", "5");
      staging.appendCodingProperty(1, "assoc", "A");
      staging.appendDenseEdge(1, 0);
      staging.appendCodeEdge(1, "child", "A");
      staging.sealForReading();

      final List<Row> concepts =
          spark
              .read()
              .schema(CodeSystemStaging.conceptSchema())
              .json(staging.conceptPath())
              .orderBy(COLUMN_CODE)
              .collectAsList();
      assertEquals(2, concepts.size());
      assertEquals("Alpha", concepts.get(0).getAs(TerminologyStoreSchema.COLUMN_DISPLAY));
      assertTrue((Boolean) concepts.get(0).getAs(TerminologyStoreSchema.COLUMN_ACTIVE));
      assertFalse((Boolean) concepts.get(1).getAs(TerminologyStoreSchema.COLUMN_ACTIVE));

      final List<Row> descriptions =
          spark
              .read()
              .schema(CodeSystemStaging.descriptionSchema())
              .json(staging.descriptionPath())
              .collectAsList();
      assertEquals(1, descriptions.size());
      assertEquals("AlphaSyn", descriptions.get(0).getAs(COLUMN_TERM));

      final List<Row> properties =
          spark
              .read()
              .schema(CodeSystemStaging.propertySchema())
              .json(staging.propertyPath())
              .collectAsList();
      assertEquals(1, properties.size());
      assertEquals("5", properties.get(0).getAs(TerminologyStoreSchema.COLUMN_VALUE));

      final List<Row> codingProperties =
          spark
              .read()
              .schema(CodeSystemStaging.codingPropertySchema())
              .json(staging.codingPropertyPath())
              .collectAsList();
      assertEquals(1, codingProperties.size());
      assertEquals("A", codingProperties.get(0).getAs(COLUMN_TARGET_CODE));

      final List<Row> denseEdges =
          spark
              .read()
              .schema(CodeSystemStaging.denseEdgeSchema())
              .json(staging.denseEdgePath())
              .collectAsList();
      assertEquals(1, denseEdges.size());
      assertEquals(0, (int) denseEdges.get(0).getAs(TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID));

      final List<Row> codeEdges =
          spark
              .read()
              .schema(CodeSystemStaging.codeEdgeSchema())
              .json(staging.codeEdgePath())
              .collectAsList();
      assertEquals(1, codeEdges.size());
      assertEquals("child", codeEdges.get(0).getAs(CodeSystemStaging.COLUMN_KNOWN_ROLE));
    }
    // The directory is deleted on the success path when the staging is closed.
    assertFalse(Files.exists(directory));
  }

  @Test
  void readsEmptyStagingFilesAsZeroRows() {
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      staging.sealForReading();
      final long count =
          spark
              .read()
              .schema(CodeSystemStaging.conceptSchema())
              .json(staging.conceptPath())
              .count();
      assertEquals(0, count);
    }
  }

  @Test
  void deletesTheDirectoryOnTheFailurePath() {
    Path directory = null;
    try (CodeSystemStaging staging = CodeSystemStaging.create()) {
      directory = staging.getDirectory();
      staging.appendConcept("A", 0, true, false, "Alpha");
      // Simulate a failure mid-import before the staging is sealed.
      throw new IllegalStateException("simulated failure");
    } catch (final IllegalStateException expected) {
      // The try-with-resources close runs before this handler.
    }
    assertFalse(Files.exists(directory));
  }
}

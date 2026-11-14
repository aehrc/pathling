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

package au.csiro.pathling.library.io.source;

import static au.csiro.pathling.library.io.DivergentBatches.RESOURCE_TYPE;
import static au.csiro.pathling.library.io.DivergentBatches.byId;
import static au.csiro.pathling.library.io.DivergentBatches.nameFields;
import static au.csiro.pathling.library.io.DivergentBatches.names;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.DivergentBatches;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that reading raw Parquet files whose schemas differ returns the union of their elements,
 * with no resource lost (US5, FR-041, SC-005).
 *
 * <p>Without merging, Spark takes the schema of whichever file it lists first and null-fills every
 * column the other files carry, silently. The fixture diverges in both directions, so that this
 * loses columns whichever file is listed first.
 *
 * @author Piotr Szul
 */
class SchemaMergingTest {

  private static SparkSession spark;
  private static PathlingContext context;

  @TempDir Path warehouse;

  /** Set up Spark and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark = TestHelpers.spark();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /** The files really do diverge, so that the tests below cannot pass by accident. */
  @Test
  void theFixtureFilesDivergeInBothDirections() {
    final String table = DivergentBatches.writeParquet(context, warehouse.toString());

    final Dataset<Row> merged = spark.read().option("mergeSchema", "true").parquet(table);
    final List<List<String>> fileFields =
        Arrays.stream(merged.inputFiles())
            .map(file -> Arrays.asList(spark.read().parquet(file).schema().fieldNames()))
            .toList();

    assertEquals(2, fileFields.size());
    assertTrue(
        fileFields.stream()
            .anyMatch(fields -> fields.contains("gender") && !fields.contains("birthDate")));
    assertTrue(
        fileFields.stream()
            .anyMatch(fields -> fields.contains("birthDate") && !fields.contains("gender")));
  }

  /** The dataset carries every element either file carries, at the top level and nested. */
  @Test
  void readingDivergentFilesReturnsTheUnionOfTheirElements() {
    DivergentBatches.writeParquet(context, warehouse.toString());

    final Dataset<Row> patients = context.read().parquet(warehouse.toString()).read(RESOURCE_TYPE);

    assertTrue(
        Arrays.asList(patients.schema().fieldNames())
            .containsAll(List.of("id", "gender", "birthDate", "name")));
    assertTrue(nameFields(patients.schema()).containsAll(List.of("family", "given")));
  }

  /**
   * Every resource of both files is returned, carrying its own values, with the elements its file
   * lacked null.
   */
  @Test
  void readingDivergentFilesLosesNoResourceAndNoValue() {
    DivergentBatches.writeParquet(context, warehouse.toString());

    final Dataset<Row> patients = context.read().parquet(warehouse.toString()).read(RESOURCE_TYPE);

    final Map<String, String> genders = byId(patients, "gender");
    final Map<String, String> birthDates = byId(patients, "birthDate");
    assertEquals(Set.of("p1", "p2", "p3", "p4"), genders.keySet());
    assertEquals("male", genders.get("p1"));
    assertEquals("female", genders.get("p2"));
    assertNull(genders.get("p3"));
    assertEquals("1990-01-01", birthDates.get("p3"));
    assertEquals("1985-06-15", birthDates.get("p4"));
    assertNull(birthDates.get("p1"));

    // The repeating element keeps its cardinality in both batches, and each leaf is populated
    // where its batch carried it.
    assertEquals(
        List.of("p1:Alpha:", "p1:Beta:", "p2:Gamma:", "p3::Delta", "p4::Epsilon Zeta", "p4::Eta"),
        names(patients));
  }

  /**
   * A supplied schema is used as given, so the files' footers need not be merged on every read, and
   * one that covers the union of the files returns everything a merge would (T118).
   */
  @Test
  void aSuppliedSchemaIsUsedInsteadOfMerging() {
    final String table = DivergentBatches.writeParquet(context, warehouse.toString());
    final StructType union = spark.read().option("mergeSchema", "true").parquet(table).schema();

    final Dataset<Row> patients =
        context
            .read()
            .parquetWithSchemas(warehouse.toString(), Map.of(RESOURCE_TYPE, union))
            .read(RESOURCE_TYPE);

    assertEquals(union, patients.schema());
    assertEquals(
        List.of("p1:Alpha:", "p1:Beta:", "p2:Gamma:", "p3::Delta", "p4::Epsilon Zeta", "p4::Eta"),
        names(patients));
  }

  /**
   * A supplied schema narrower than the files is still used as given, and the columns it omits are
   * not read. This is why the opt-out is safe only while the schema covers the union of the files:
   * it trades the merge for the caller's guarantee, deterministically rather than by listing order.
   */
  @Test
  void aSuppliedSchemaNarrowerThanTheFilesIsUsedAsGiven() {
    DivergentBatches.writeParquet(context, warehouse.toString());
    final StructType firstOnly = DivergentBatches.first(context).schema();

    final Dataset<Row> patients =
        context
            .read()
            .parquetWithSchemas(warehouse.toString(), Map.of(RESOURCE_TYPE, firstOnly))
            .read(RESOURCE_TYPE);

    assertEquals(firstOnly, patients.schema());
    assertEquals(Set.of("p1", "p2", "p3", "p4"), byId(patients, "gender").keySet());
  }

  /** A resource type the supplied schemas do not name is merged as usual. */
  @Test
  void typesWithoutASuppliedSchemaAreMerged() {
    DivergentBatches.writeParquet(context, warehouse.toString());
    final StructType unrelated = DivergentBatches.first(context).schema();

    final Dataset<Row> patients =
        context
            .read()
            .parquetWithSchemas(warehouse.toString(), Map.of("Observation", unrelated))
            .read(RESOURCE_TYPE);

    assertTrue(
        Arrays.asList(patients.schema().fieldNames()).containsAll(List.of("gender", "birthDate")));
    assertTrue(nameFields(patients.schema()).containsAll(List.of("family", "given")));
  }
}

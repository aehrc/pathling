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

package au.csiro.pathling.library.io.sink;

import static au.csiro.pathling.library.io.DivergentBatches.RESOURCE_TYPE;
import static au.csiro.pathling.library.io.DivergentBatches.byId;
import static au.csiro.pathling.library.io.DivergentBatches.nameFields;
import static au.csiro.pathling.library.io.DivergentBatches.names;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.DivergentBatches;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that writing a batch whose schema differs from a Delta table's succeeds and widens the
 * table, both on append (US5, FR-042, SC-005) and on upsert (decision 41).
 *
 * <p>Under a schema fitted to the data, a batch carrying elements the table has not seen before is
 * the steady state rather than an error, so neither write path may refuse it, and the two must not
 * give different answers to the same question.
 *
 * @author Piotr Szul
 */
class DeltaSchemaMergingTest {

  private static SparkSession spark;
  private static PathlingContext context;

  private static final String QUANTITY_OF_1_G =
      """
      {"resourceType":"Observation","id":"o1","status":"final","code":{"text":"weight"},
       "valueQuantity":{"value":1,"unit":"g","system":"http://unitsofmeasure.org","code":"g"}}\
      """;

  private static final String QUANTITY_OF_2_KG =
      """
      {"resourceType":"Observation","id":"o1","status":"final","code":{"text":"weight"},
       "valueQuantity":{"value":2,"unit":"kg","system":"http://unitsofmeasure.org","code":"kg"}}\
      """;

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

  /** An append carrying elements the table lacks succeeds, and the table gains them. */
  @Test
  void appendingADivergentBatchWidensTheTable() {
    write(DivergentBatches.first(context), "overwrite");

    write(DivergentBatches.second(context), "append");

    final Dataset<Row> table = storedTable();
    assertTrue(
        Arrays.asList(table.schema().fieldNames()).containsAll(List.of("gender", "birthDate")));
    assertTrue(nameFields(table.schema()).containsAll(List.of("family", "given")));
  }

  /** Every resource of both batches is in the widened table, with its own values. */
  @Test
  void appendingADivergentBatchLosesNoResourceAndNoValue() {
    write(DivergentBatches.first(context), "overwrite");

    write(DivergentBatches.second(context), "append");

    final Dataset<Row> patients = context.read().delta(warehouse.toString()).read(RESOURCE_TYPE);
    final Map<String, String> genders = byId(patients, "gender");
    assertEquals(Set.of("p1", "p2", "p3", "p4"), genders.keySet());
    assertEquals("male", genders.get("p1"));
    assertEquals("female", genders.get("p2"));
    final Map<String, String> birthDates = byId(patients, "birthDate");
    assertEquals("1990-01-01", birthDates.get("p3"));
    assertEquals("1985-06-15", birthDates.get("p4"));
    assertEquals(
        List.of("p1:Alpha:", "p1:Beta:", "p2:Gamma:", "p3::Delta", "p4::Epsilon Zeta", "p4::Eta"),
        names(patients));
  }

  /**
   * An upsert whose batch both lacks elements the table carries and carries elements it lacks
   * succeeds, and the table gains them, as an append does (decision 41).
   */
  @Test
  void upsertingADivergentBatchWidensTheTable() {
    write(DivergentBatches.first(context), "overwrite");

    write(DivergentBatches.secondUpdatingFirst(context), "merge");

    final Dataset<Row> table = storedTable();
    assertTrue(
        Arrays.asList(table.schema().fieldNames()).containsAll(List.of("gender", "birthDate")));
    assertTrue(nameFields(table.schema()).containsAll(List.of("family", "given")));
  }

  /**
   * The upsert replaces a matched resource in full, so none of the elements the earlier version
   * carried and the new one does not survive, while an unmatched resource is left untouched.
   */
  @Test
  void upsertingADivergentBatchReplacesMatchedResourcesInFull() {
    write(DivergentBatches.first(context), "overwrite");

    write(DivergentBatches.secondUpdatingFirst(context), "merge");

    final Dataset<Row> patients = context.read().delta(warehouse.toString()).read(RESOURCE_TYPE);
    final Map<String, String> genders = byId(patients, "gender");
    assertEquals(Set.of("p1", "p2", "p3"), genders.keySet());
    // The replaced resource has no gender, because its new version has none.
    assertNull(genders.get("p1"));
    assertEquals("female", genders.get("p2"));
    final Map<String, String> birthDates = byId(patients, "birthDate");
    assertEquals("1970-03-03", birthDates.get("p1"));
    assertEquals("1990-01-01", birthDates.get("p3"));
    assertEquals(List.of("p1::Updated", "p2:Gamma:", "p3::Delta"), names(patients));
  }

  /**
   * An upsert that brings the exact canonical value of a quantity into a table that lacks it stores
   * that value with its fields in the order of its type, so that a comparison read back from the
   * table, which takes them by position, still works.
   */
  @Test
  void upsertingAnAnnotatedBatchKeepsTheExactCanonicalValueReadable() {
    write("Observation", observations(List.of(QUANTITY_OF_1_G), false), "overwrite");

    write("Observation", observations(List.of(QUANTITY_OF_2_KG), true), "merge");

    final StructType quantity =
        (StructType) storedTable("Observation").schema().apply("valueQuantity").dataType();
    assertEquals(
        List.of("value", "scale"),
        Arrays.asList(
            ((StructType) quantity.apply("__canonical_value_exact").dataType()).fieldNames()));
    final List<Row> heavier =
        context
            .read()
            .delta(warehouse.toString())
            .view("Observation")
            .json(
                """
                {"resource":"Observation","select":[{"column":[
                  {"name":"id","path":"id"},
                  {"name":"heavier","path":"value.ofType(Quantity) > 1500 'g'"}]}]}\
                """)
            .execute()
            .collectAsList();
    assertEquals("[[o1,true]]", heavier.toString());
  }

  private void write(@Nonnull final Dataset<Row> batch, @Nonnull final String saveMode) {
    write(RESOURCE_TYPE, batch, saveMode);
  }

  private void write(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> batch,
      @Nonnull final String saveMode) {
    context
        .read()
        .datasets()
        .dataset(resourceType, batch)
        .write()
        .saveMode(saveMode)
        .delta(warehouse.toString());
  }

  @Nonnull
  private static Dataset<Row> observations(
      @Nonnull final List<String> json, final boolean annotated) {
    final Dataset<Row> observations =
        FhirReader.of(spark, FhirDefinitionContext.of(context.getFhirContext()))
            .json()
            .read("Observation", spark.createDataset(json, Encoders.STRING()));
    return annotated ? context.annotate(observations, "Observation") : observations;
  }

  @Nonnull
  private Dataset<Row> storedTable() {
    return storedTable(RESOURCE_TYPE);
  }

  @Nonnull
  private Dataset<Row> storedTable(@Nonnull final String resourceType) {
    return spark
        .read()
        .format("delta")
        .load(warehouse.resolve(resourceType + ".parquet").toString());
  }
}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that appending a batch whose schema differs from a Delta table's succeeds and widens the
 * table (US5, FR-042, SC-005).
 *
 * <p>Under a schema fitted to the data, a batch carrying elements the table has not seen before is
 * the steady state rather than an error.
 *
 * @author Piotr Szul
 */
class DeltaSchemaMergingTest {

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

  private void write(@Nonnull final Dataset<Row> batch, @Nonnull final String saveMode) {
    context
        .read()
        .datasets()
        .dataset(RESOURCE_TYPE, batch)
        .write()
        .saveMode(saveMode)
        .delta(warehouse.toString());
  }

  @Nonnull
  private Dataset<Row> storedTable() {
    return spark
        .read()
        .format("delta")
        .load(warehouse.resolve(RESOURCE_TYPE + ".parquet").toString());
  }
}

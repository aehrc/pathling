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
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.DivergentBatches;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a view unnesting a repeating element and projecting a leaf only one batch populated
 * returns the resources of both batches, read through the library's own sources (US5 scenario 2,
 * FR-036, SC-005, QS-006).
 *
 * <p>This extends T026–T027, which pin the same constraint in the engine over files it is handed,
 * to the source and sink level: the batches are written as a directory of raw Parquet files and as
 * a Delta table built by an append, and read back through {@code ParquetSource} and {@code
 * DeltaSource}. It is the scenario that loses rows silently if unnesting is ever reshaped into a
 * read of the single leaf.
 *
 * @author Piotr Szul
 */
class DivergentFileViewTest {

  /**
   * Unnests {@code name} and projects {@code family} alone, which only the first batch populates.
   */
  private static final String VIEW =
      """
      {
        "resourceType": "ViewDefinition",
        "resource": "Patient",
        "status": "active",
        "select": [
          {
            "column": [{"name": "id", "path": "id"}]
          },
          {
            "forEach": "name",
            "column": [{"name": "family", "path": "family", "type": "string"}]
          }
        ]
      }
      """;

  /** One row per name: the first batch's families, and a null for each name of the second. */
  private static final List<String> EXPECTED =
      List.of("p1:Alpha", "p1:Beta", "p2:Gamma", "p3:null", "p4:null", "p4:null");

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

  /** Raw Parquet files, read as one dataset by the Parquet source. */
  @Test
  void viewOverDivergentParquetFilesReturnsResourcesFromBothBatches() {
    DivergentBatches.writeParquet(context, warehouse.toString());

    final QueryableDataSource source = context.read().parquet(warehouse.toString());

    assertEquals(EXPECTED, families(source));
  }

  /** A Delta table built by appending the second batch to the first, read by the Delta source. */
  @Test
  void viewOverDivergentDeltaTableReturnsResourcesFromBothBatches() {
    context
        .read()
        .datasets()
        .dataset(RESOURCE_TYPE, DivergentBatches.first(context))
        .write()
        .saveMode("overwrite")
        .delta(warehouse.toString());
    context
        .read()
        .datasets()
        .dataset(RESOURCE_TYPE, DivergentBatches.second(context))
        .write()
        .saveMode("append")
        .delta(warehouse.toString());

    final QueryableDataSource source = context.read().delta(warehouse.toString());

    assertEquals(EXPECTED, families(source));
  }

  @Nonnull
  private static List<String> families(@Nonnull final QueryableDataSource source) {
    return source.view(RESOURCE_TYPE).json(VIEW).execute().collectAsList().stream()
        .map(row -> row.getString(0) + ":" + Objects.toString(row.get(1)))
        .sorted()
        .toList();
  }
}

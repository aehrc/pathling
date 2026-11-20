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
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.DivergentBatches;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a file-based source resolves the schema of a resource type only when that type is
 * read, rather than resolving every type in the warehouse when it is constructed (T118b).
 *
 * <p>With schema merging on, resolving a type reads the footer of every one of its files, so doing
 * it eagerly multiplies that cost by the number of types in the warehouse before a single query
 * runs. The tests detect eager resolution through a type whose only file is not Parquet at all,
 * which fails as soon as its schema is resolved.
 *
 * @author Piotr Szul
 */
class LazySchemaResolutionTest {

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

  /** A warehouse holding one readable type and one whose file cannot be read as Parquet. */
  @BeforeEach
  void writeWarehouse() throws IOException {
    DivergentBatches.first(context)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(warehouse.resolve(RESOURCE_TYPE + ".parquet").toString());
    Files.writeString(
        warehouse.resolve("Observation.parquet"), "not a Parquet file", StandardCharsets.UTF_8);
  }

  /** Constructing the source resolves no schema, so the unreadable type does not stop it. */
  @Test
  void constructingTheSourceResolvesNoSchema() {
    final QueryableDataSource source = context.read().parquet(warehouse.toString());

    assertEquals(Set.of(RESOURCE_TYPE, "Observation"), source.getResourceTypes());
  }

  /** A type can be read without resolving any other. */
  @Test
  void readingOneTypeResolvesOnlyThatType() {
    final QueryableDataSource source = context.read().parquet(warehouse.toString());

    assertEquals(2, source.read(RESOURCE_TYPE).count());
  }

  /** The unreadable type still fails, at the point it is read. */
  @Test
  void anUnreadableTypeFailsWhenItIsRead() {
    final QueryableDataSource source = context.read().parquet(warehouse.toString());

    assertThrows(Exception.class, () -> source.read("Observation"));
  }

  /** Filtering the source by type resolves only the types it keeps. */
  @Test
  void filteringByTypeResolvesNoSchema() {
    final QueryableDataSource filtered =
        context.read().parquet(warehouse.toString()).filterByResourceType(RESOURCE_TYPE::equals);

    assertEquals(Set.of(RESOURCE_TYPE), filtered.getResourceTypes());
    assertEquals(2, filtered.read(RESOURCE_TYPE).count());
  }
}

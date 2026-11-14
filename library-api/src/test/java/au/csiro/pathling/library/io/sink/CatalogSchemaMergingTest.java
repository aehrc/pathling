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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that appending into an existing catalog table whose schema differs from the batch's widens
 * a Delta table, as every other write into an existing Delta table does, and refuses before
 * anything is written for a table in a format that cannot be widened (decision 86, FR-042).
 *
 * <p>A Parquet catalog table's stored schema cannot gain a nested field through any public route,
 * and Spark refuses a differing append at every level anyway, narrower included. The refusal here
 * replaces Spark's storage-layer error with one naming the table, the difference and the remedy.
 *
 * @author Piotr Szul
 */
class CatalogSchemaMergingTest {

  private static final String RESOURCE_TYPE = "Patient";

  /** The schema of the batch that creates each table. */
  private static final String BASE =
      "SELECT 'a' AS id, 'male' AS gender,"
          + " named_struct('versionId', '1', 'source', 's1') AS meta,"
          + " array(named_struct('family', 'F1', 'text', 'T1')) AS name";

  @TempDir static Path warehouse;

  private static SparkSession spark;
  private static PathlingContext context;

  private String database;

  /** Set up Spark, with a warehouse in a temporary directory, and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.warehouse.dir", warehouse.resolve("spark-warehouse").toString())
            .getOrCreate();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /** Give each test a database of its own, so that tests do not share tables. */
  @BeforeEach
  void createDatabase() {
    database = "db_" + UUID.randomUUID().toString().replace("-", "");
    spark.sql("CREATE DATABASE " + database);
  }

  /** A Delta table gains a top-level column only the batch carries. */
  @Test
  void deltaAppendWidensAtTheTopLevel() {
    create("delta");

    append(
        "SELECT 'b' AS id, 'female' AS gender, '1990-01-01' AS birthDate,"
            + " named_struct('versionId', '2', 'source', 's2') AS meta,"
            + " array(named_struct('family', 'F2', 'text', 'T2')) AS name",
        "delta");

    assertTrue(table().schema().getFieldIndex("birthDate").isDefined());
    assertEquals(2, table().count());
  }

  /** A Delta table gains a field of a struct only the batch carries. */
  @Test
  void deltaAppendWidensInsideAStruct() {
    create("delta");

    append(
        "SELECT 'b' AS id, 'female' AS gender,"
            + " named_struct('versionId', '2', 'source', 's2', 'lastUpdated', 'x') AS meta,"
            + " array(named_struct('family', 'F2', 'text', 'T2')) AS name",
        "delta");

    final StructType meta = (StructType) table().schema().apply("meta").dataType();
    assertTrue(meta.getFieldIndex("lastUpdated").isDefined());
    assertEquals(2, table().count());
  }

  /**
   * A Delta table gains a field of a struct within an array only the batch carries, from a batch
   * that also lacks a field the table has.
   */
  @Test
  void deltaAppendWidensInsideAnArrayOfStructs() {
    create("delta");

    append(
        "SELECT 'b' AS id, 'female' AS gender,"
            + " named_struct('versionId', '2', 'source', 's2') AS meta,"
            + " array(named_struct('family', 'F2', 'use', 'official')) AS name",
        "delta");

    final StructType name =
        (StructType) ((ArrayType) table().schema().apply("name").dataType()).elementType();
    assertTrue(name.getFieldIndex("use").isDefined());
    assertTrue(name.getFieldIndex("text").isDefined());
    assertEquals(2, table().count());
  }

  /** A Parquet table refuses a batch lacking a field it has, and is left as it was. */
  @Test
  void parquetAppendRefusesANarrowerBatch() {
    create("parquet");
    final StructType before = table().schema();

    final IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                append(
                    "SELECT 'b' AS id,"
                        + " named_struct('versionId', '2', 'source', 's2') AS meta,"
                        + " array(named_struct('family', 'F2', 'text', 'T2')) AS name",
                    "parquet"));

    assertRefusal(failure, "gender");
    assertEquals(before, table().schema());
    assertEquals(1, table().count());
  }

  /** A Parquet table refuses a batch carrying a nested field it lacks, and is left as it was. */
  @Test
  void parquetAppendRefusesAWiderBatch() {
    create("parquet");
    final StructType before = table().schema();

    final IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                append(
                    "SELECT 'b' AS id, 'female' AS gender,"
                        + " named_struct('versionId', '2', 'source', 's2') AS meta,"
                        + " array(named_struct('family', 'F2', 'text', 'T2', 'use', 'u'))"
                        + " AS name",
                    "parquet"));

    assertRefusal(failure, "name[].use");
    assertEquals(before, table().schema());
    assertEquals(1, table().count());
  }

  /** A Parquet table takes a batch whose schema is identical to its own. */
  @Test
  void parquetAppendTakesAnIdenticalBatch() {
    create("parquet");

    assertDoesNotThrow(
        () ->
            append(
                "SELECT 'b' AS id, 'female' AS gender,"
                    + " named_struct('versionId', '2', 'source', 's2') AS meta,"
                    + " array(named_struct('family', 'F2', 'text', 'T2')) AS name",
                "parquet"));

    assertEquals(2, table().count());
  }

  /**
   * A Parquet table takes a batch carrying the same fields in another order, because an append
   * places fields by name.
   */
  @Test
  void parquetAppendTakesAReorderedBatch() {
    create("parquet");

    assertDoesNotThrow(
        () ->
            append(
                "SELECT array(named_struct('text', 'T2', 'family', 'F2')) AS name,"
                    + " named_struct('source', 's2', 'versionId', '2') AS meta,"
                    + " 'female' AS gender, 'b' AS id",
                "parquet"));

    assertEquals(2, table().count());
    assertEquals("F2", table().where("id = 'b'").selectExpr("name[0].family").first().getString(0));
  }

  private void assertRefusal(
      @Nonnull final IllegalArgumentException failure, @Nonnull final String difference) {
    final String message = failure.getMessage();
    assertTrue(message.contains(tableName()), "should name the table: " + message);
    assertTrue(message.contains(difference), "should name the difference: " + message);
    assertTrue(message.contains("Delta"), "should name the remedy: " + message);
  }

  private void create(@Nonnull final String format) {
    write(spark.sql(BASE), "error", format);
  }

  private void append(@Nonnull final String batch, @Nonnull final String format) {
    write(spark.sql(batch), "append", format);
  }

  private void write(
      @Nonnull final Dataset<Row> batch,
      @Nonnull final String saveMode,
      @Nonnull final String format) {
    context
        .read()
        .datasets()
        .dataset(RESOURCE_TYPE, batch)
        .write()
        .saveMode(saveMode)
        .tables(database, format);
  }

  @Nonnull
  private String tableName() {
    return database + "." + RESOURCE_TYPE;
  }

  @Nonnull
  private Dataset<Row> table() {
    spark.catalog().refreshTable(tableName());
    return spark.table(tableName());
  }
}

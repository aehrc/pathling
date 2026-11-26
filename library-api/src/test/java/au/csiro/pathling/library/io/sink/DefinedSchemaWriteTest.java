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

import static au.csiro.pathling.library.io.DivergentBatches.byId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.SchemaEquality;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests writing data loaded with a schema set through the sinks (T020, US1).
 *
 * <p>Each batch is NDJSON read with a schema set, so its shape is the set's, whatever the resources
 * of the batch populate.
 */
class DefinedSchemaWriteTest {

  @Nonnull private static final String PATIENT = "Patient";

  /** Patients that differ from the defined schema in both directions. */
  @Nonnull
  private static final List<String> DIVERGENT =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"active\":true,\"gender\":\"female\","
              + "\"name\":[{\"given\":[\"Ann\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"1990-01-01\"}");

  @TempDir static Path warehouse;

  private static SparkSession spark;
  private static PathlingContext context;

  @TempDir Path directory;

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

  /**
   * The Parquet, Delta and catalog sinks store a type loaded with a schema set with the set's
   * schema (US1 scenario 5, SC-003).
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "delta", "tables"})
  void aSinkWritesTheSchemaOfTheSet(@Nonnull final String sink) {
    final StructType schema =
        schemaOf(
            "{\"resourceType\":\"Patient\",\"id\":\"d\",\"gender\":\"male\","
                + "\"birthDate\":\"2000-01-01\",\"name\":[{\"family\":\"Smith\"}]}");
    final QueryableDataSource source = load(DIVERGENT, schema);

    final Dataset<Row> stored =
        switch (sink) {
          case "parquet" -> {
            source.write().parquet(directory.toString());
            yield spark.read().parquet(directory.resolve("Patient.parquet").toString());
          }
          case "delta" -> {
            source.write().delta(directory.toString());
            yield spark
                .read()
                .format("delta")
                .load(directory.resolve("Patient.parquet").toString());
          }
          default -> {
            final String database = "db_" + UUID.randomUUID().toString().replace("-", "");
            spark.sql("CREATE DATABASE " + database);
            source.write().tables(database);
            yield spark.table(database + "." + PATIENT);
          }
        };

    assertSame(schema, stored.schema());
  }

  /**
   * Three Delta appends of differing batches, each loaded with the table's schema set under the
   * configuration the table was written with, keep the table's schema, and every resource is kept
   * (SC-002).
   */
  @Test
  void appendsLoadedWithTheTablesSchemaSetKeepTheTablesSchema() {
    load(List.of(
            "{\"resourceType\":\"Patient\",\"id\":\"p0\",\"gender\":\"male\","
                + "\"birthDate\":\"1970-01-01\","
                + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Bob\"]}]}"))
        .write()
        .delta(directory.toString());
    final StructType tableSchema = storedDelta().schema();
    final List<List<String>> batches =
        List.of(
            List.of("{\"resourceType\":\"Patient\",\"id\":\"p1\",\"active\":true}"),
            List.of(
                "{\"resourceType\":\"Patient\",\"id\":\"p2\","
                    + "\"address\":[{\"city\":\"Brisbane\"}]}"),
            List.of(
                "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"gender\":\"female\","
                    + "\"telecom\":[{\"value\":\"555\"}]}"));

    batches.forEach(
        batch -> {
          load(batch, tableSchema).write().saveMode("append").delta(directory.toString());

          assertSame(tableSchema, storedDelta().schema());
        });
    assertEquals(4, storedDelta().count());
  }

  /**
   * A Delta merge of data loaded with a schema narrower than the table replaces each matched
   * resource in full, so the elements the schema lacks are null on the matched rows, and leaves the
   * table's schema and its other rows as they were (spec edge case "A Delta merge (upsert) with a
   * schema narrower than the table").
   */
  @Test
  void aMergeLoadedWithANarrowerSchemaNullsTheAbsentElementsOfMatchedRows() {
    load(List.of(
            "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"gender\":\"male\","
                + "\"birthDate\":\"1970-01-01\"}",
            "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"gender\":\"female\","
                + "\"birthDate\":\"1980-01-01\"}"))
        .write()
        .delta(directory.toString());
    final StructType before = storedDelta().schema();
    final StructType narrow =
        schemaOf("{\"resourceType\":\"Patient\",\"id\":\"d\",\"birthDate\":\"2000-01-01\"}");

    load(
            List.of(
                "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"gender\":\"other\","
                    + "\"birthDate\":\"1971-01-01\"}"),
            narrow)
        .write()
        .saveMode("merge")
        .delta(directory.toString());

    final Dataset<Row> after = storedDelta();
    assertSame(before, after.schema());
    final Map<String, String> genders = byId(after, "gender");
    assertNull(genders.get("p1"));
    assertEquals("female", genders.get("p2"));
    assertEquals("1971-01-01", byId(after, "birthDate").get("p1"));
  }

  /**
   * An append of data loaded with a schema wider than the table's widens the table, as an inferred
   * append does (spec edge case "An append through a Pathling sink of data whose schema differs
   * from the table's").
   */
  @Test
  void anAppendLoadedWithAWiderSchemaWidensTheTable() {
    final StructType narrow =
        schemaOf("{\"resourceType\":\"Patient\",\"id\":\"d\",\"gender\":\"male\"}");
    final StructType wide =
        schemaOf(
            "{\"resourceType\":\"Patient\",\"id\":\"d\",\"gender\":\"male\","
                + "\"birthDate\":\"2000-01-01\"}");
    load(List.of("{\"resourceType\":\"Patient\",\"id\":\"p1\",\"gender\":\"male\"}"), narrow)
        .write()
        .delta(directory.toString());

    load(List.of("{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"1990-01-01\"}"), wide)
        .write()
        .saveMode("append")
        .delta(directory.toString());

    final Dataset<Row> table = storedDelta();
    assertTrue(Arrays.asList(table.schema().fieldNames()).contains("birthDate"));
    assertEquals("1990-01-01", byId(table, "birthDate").get("p2"));
    assertEquals("male", byId(table, "gender").get("p1"));
  }

  /** Returns the PoF schema, without annotations, that a Patient is read with when inferred. */
  @Nonnull
  private static StructType schemaOf(@Nonnull final String patient) {
    return FhirReader.of(
            spark,
            FhirDefinitionContext.of(context.getFhirContext()),
            context.getSchemaConfiguration())
        .json()
        .read(PATIENT, spark.createDataset(List.of(patient), Encoders.STRING()))
        .schema();
  }

  /** Returns a source over NDJSON Patients, read with a schema set naming Patient. */
  @Nonnull
  private QueryableDataSource load(
      @Nonnull final List<String> patients, @Nonnull final StructType schema) {
    return context.read().withSchemas(Map.of(PATIENT, schema)).ndjson(ndjson(patients));
  }

  /** Returns a source over NDJSON Patients, read without a schema set. */
  @Nonnull
  private QueryableDataSource load(@Nonnull final List<String> patients) {
    return context.read().ndjson(ndjson(patients));
  }

  /** Writes Patients to a directory of their own, and returns it. */
  @Nonnull
  private String ndjson(@Nonnull final List<String> patients) {
    try {
      final Path batch = Files.createDirectory(directory.resolve("batch-" + UUID.randomUUID()));
      Files.writeString(batch.resolve("Patient.ndjson"), String.join("\n", patients));
      return batch.toString();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private Dataset<Row> storedDelta() {
    return spark.read().format("delta").load(directory.resolve("Patient.parquet").toString());
  }

  private static void assertSame(
      @Nonnull final StructType expected, @Nonnull final StructType actual) {
    assertTrue(
        SchemaEquality.same(expected, actual),
        () -> "expected " + expected.treeString() + " but got " + actual.treeString());
  }
}

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

import static au.csiro.pathling.library.io.source.SchemaSetSources.CONDITION;
import static au.csiro.pathling.library.io.source.SchemaSetSources.PATIENT;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.SchemaSets;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.DivergentBatches;
import au.csiro.pathling.library.io.SchemaEquality;
import au.csiro.pathling.test.JobCountingListener;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that every source reports the schema set it holds, that the set survives a schema set file
 * into every source and into encoding, and that a schema set canonicalises to canonical order
 * (T037, T057, FR-005, FR-017 to FR-019, FR-027, SC-007, SC-009).
 */
class SourceSchemasTest {

  /** Every kind of source. */
  @Nonnull
  private static final List<String> KINDS =
      List.of(
          SchemaSetSources.NDJSON,
          SchemaSetSources.BUNDLES_JSON,
          SchemaSetSources.BUNDLES_XML,
          SchemaSetSources.PARQUET,
          SchemaSetSources.DELTA,
          SchemaSetSources.TABLES,
          SchemaSetSources.BULK,
          SchemaSetSources.DATASETS);

  @TempDir static Path warehouse;

  private static SparkSession spark;
  private static PathlingContext context;

  /** The directory under which every kind of source is written once, for the whole class. */
  private static Path sources;

  /** The catalog database the shared table source is written to. */
  private static String sharedDatabase;

  @TempDir Path directory;

  /** Set up Spark, with a warehouse in a temporary directory, and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.warehouse.dir", warehouse.resolve("spark-warehouse").toString())
            .getOrCreate();
    context = PathlingContext.create(spark);
    // Every kind of source is written once, since writing all of them for each test that reads
    // them would make this the slowest class in the module.
    sources = warehouse.resolve("sources");
    sharedDatabase = "db_shared";
    spark.sql("CREATE DATABASE " + sharedDatabase);
    KINDS.forEach(kind -> SchemaSetSources.write(kind, context, place(kind), sharedDatabase));
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /**
   * A source reports one schema for each type it holds, which is the schema a read of that type
   * gives (FR-017, US4 scenario 1).
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "ndjson",
        "bundlesJson",
        "bundlesXml",
        "parquet",
        "delta",
        "tables",
        "bulk",
        "datasets"
      })
  void reportsOneSchemaForEachTypeHeld(@Nonnull final String kind) {
    final QueryableDataSource source = shared(kind, context.read());

    final Map<String, StructType> schemas = source.schemas();

    assertEquals(Set.of(PATIENT, CONDITION), schemas.keySet());
    assertEquals(source.read(PATIENT).schema(), schemas.get(PATIENT));
    assertEquals(source.read(CONDITION).schema(), schemas.get(CONDITION));
  }

  /**
   * A Parquet source whose files differ reports the merged union of their schemas, in whatever
   * order Spark reports it, so the fields are compared without regard to order (FR-018, US4
   * scenario 2).
   */
  @Test
  void parquetSourceWithDifferingFilesReportsTheMergedSchema() {
    final String table = DivergentBatches.writeParquet(context, directory.toString());
    final StructType union = spark.read().option("mergeSchema", "true").parquet(table).schema();

    final QueryableDataSource source = context.read().parquet(directory.toString());
    final StructType reported = source.schemas().get(DivergentBatches.RESOURCE_TYPE);

    assertEquals(fieldPaths(union), fieldPaths(reported));
    assertEquals(source.read(DivergentBatches.RESOURCE_TYPE).schema(), reported);
  }

  /**
   * The schema set of every source, written to a schema set file and read back, is accepted by
   * every source, and each source then reports a set that is the same as it (FR-019, SC-007, US4
   * scenario 3).
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "ndjson",
        "bundlesJson",
        "bundlesXml",
        "parquet",
        "delta",
        "tables",
        "bulk",
        "datasets"
      })
  void schemaSetRoundTripsThroughAFileIntoEverySource(@Nonnull final String kind) {
    final Map<String, StructType> schemas = shared(kind, context.read()).schemas();

    final Map<String, StructType> readBack = SchemaSets.fromJson(SchemaSets.toJson(schemas));

    assertAll(
        KINDS.stream()
            .map(
                target ->
                    (Executable)
                        () -> {
                          final Map<String, StructType> reported =
                              shared(target, context.read().withSchemas(readBack)).schemas();
                          assertTrue(
                              SchemaEquality.sameSet(readBack, reported),
                              () -> kind + " into " + target + ": " + reported);
                        }));
  }

  /**
   * A single entry of a source's schema set, written to a schema set file and read back, is
   * accepted by {@code encode} of FHIR JSON and XML and by {@code encodeBundle} of JSON and XML
   * bundles, each of which gives output that is the same as it (FR-005).
   */
  @Test
  void singleEntryIsAcceptedByEncodeAndEncodeBundle() {
    final StructType entry =
        SchemaSets.fromJson(
                SchemaSets.toJson(shared(SchemaSetSources.NDJSON, context.read()).schemas()))
            .get(PATIENT);

    final Map<String, Dataset<Row>> encoded =
        Map.of(
            "encode JSON",
            context.encode(
                SchemaSetSources.patientJson(context), PATIENT, PathlingContext.FHIR_JSON, entry),
            "encode XML",
            context.encode(
                SchemaSetSources.patientXml(context), PATIENT, PathlingContext.FHIR_XML, entry),
            "encodeBundle JSON",
            context.encodeBundle(
                SchemaSetSources.jsonBundles(context), PATIENT, PathlingContext.FHIR_JSON, entry),
            "encodeBundle XML",
            context.encodeBundle(
                SchemaSetSources.xmlBundles(context), PATIENT, PathlingContext.FHIR_XML, entry));

    assertAll(
        encoded.entrySet().stream()
            .map(
                result ->
                    (Executable)
                        () ->
                            assertTrue(
                                SchemaEquality.same(entry, result.getValue().schema()),
                                () ->
                                    result.getKey()
                                        + ": expected "
                                        + entry.treeString()
                                        + " but got "
                                        + result.getValue().schema().treeString())));
  }

  /**
   * The schema of an evolved Delta table, whose added fields come last at each depth, canonicalises
   * to the order an inferred load of FHIR JSON gives, and a load with the canonicalised set gives
   * that order (FR-027, SC-009, US4 scenario 4).
   */
  @Test
  void evolvedDeltaSchemaCanonicalisesToTheOrderOfAnInferredLoad() {
    final String table = directory.resolve("Patient.parquet").toString();
    // The second batch is written first, so that the fields the first batch adds on evolution,
    // gender and name.family, come after fields that precede them in canonical order.
    DivergentBatches.second(context).write().format("delta").save(table);
    DivergentBatches.first(context)
        .write()
        .format("delta")
        .mode(SaveMode.Append)
        .option("mergeSchema", "true")
        .save(table);
    final StructType evolved = spark.read().format("delta").load(table).schema();
    final StructType canonical = DivergentBatches.both(context).schema();
    assertFalse(
        SchemaEquality.same(canonical, evolved),
        "the evolved schema should be out of canonical order");

    final Map<String, StructType> canonicalised =
        context.canonicalSchemas(Map.of(PATIENT, evolved));

    assertEquals(Set.of(PATIENT), canonicalised.keySet());
    assertTrue(
        SchemaEquality.same(canonical, canonicalised.get(PATIENT)),
        () ->
            "expected "
                + canonical.treeString()
                + " but got "
                + canonicalised.get(PATIENT).treeString());
    final Dataset<Row> loaded =
        context.read().withSchemas(canonicalised).delta(directory.toString()).read(PATIENT);
    assertTrue(
        SchemaEquality.same(canonical, loaded.schema()),
        () -> "expected " + canonical.treeString() + " but got " + loaded.schema().treeString());
  }

  /** A key that is not a resource type code is refused, naming it (FR-027). */
  @Test
  void refusesKeyThatIsNotAResourceType() {
    final Map<String, StructType> schemas =
        Map.of("Patientt", DivergentBatches.first(context).schema());

    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> context.canonicalSchemas(schemas));
    assertTrue(error.getMessage().contains("Patientt"), error.getMessage());
  }

  /** Canonicalisation reads no data, so it runs no Spark job (FR-027). */
  @Test
  void canonicalisesWithoutRunningAJob() throws TimeoutException {
    final StructType schema = DivergentBatches.both(context).schema();
    assertEquals(
        0,
        JobCountingListener.jobsRunBy(
            spark.sparkContext(), () -> context.canonicalSchemas(Map.of(PATIENT, schema))));
  }

  /** Returns the directory a kind of source is written to, so that every kind can coexist. */
  @Nonnull
  private static Path place(@Nonnull final String kind) {
    final Path place = sources.resolve(kind);
    place.toFile().mkdirs();
    return place;
  }

  /** Opens a kind of source over the fixture written once for the class, from a builder. */
  @Nonnull
  private static QueryableDataSource shared(
      @Nonnull final String kind, @Nonnull final DataSourceBuilder builder) {
    return SchemaSetSources.open(kind, builder, place(kind), sharedDatabase);
  }

  /**
   * Returns the dotted path and type of every field of a schema, at any depth, sorted. An array
   * adds {@code []} to the path, so that a difference in cardinality is seen.
   */
  @Nonnull
  private static List<String> fieldPaths(@Nonnull final StructType schema) {
    return fieldPaths("", schema).sorted().toList();
  }

  @Nonnull
  private static Stream<String> fieldPaths(
      @Nonnull final String prefix, @Nonnull final DataType type) {
    if (type instanceof final StructType struct) {
      return Stream.of(struct.fields())
          .flatMap(
              (StructField field) -> fieldPaths(prefix + field.name() + ".", field.dataType()));
    }
    if (type instanceof final ArrayType array) {
      return fieldPaths(prefix.substring(0, prefix.length() - 1) + "[].", array.elementType());
    }
    return Stream.of(prefix + type.simpleString());
  }
}

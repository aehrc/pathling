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

import static au.csiro.pathling.library.io.DivergentBatches.byId;
import static au.csiro.pathling.library.io.SchemaEquality.annotationPaths;
import static au.csiro.pathling.library.io.source.SchemaSetSources.CONDITION;
import static au.csiro.pathling.library.io.source.SchemaSetSources.PATIENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.FhirFormatReader;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.SchemaEquality;
import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that every source given a schema set projects each resource type the set names to its
 * schema, and infers every other type as it does without one (T019, US1).
 */
class DefinedSchemaSourceTest {

  @TempDir static Path warehouse;

  private static SparkSession spark;
  private static PathlingContext context;

  @TempDir Path directory;

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

  /**
   * A named type has exactly the base schema of the set, plus the annotations the configuration
   * enables, from a batch that lacks elements of the schema and carries elements it does not have:
   * the missing elements are null and the extra content is dropped, with no error (FR-001,
   * FR-006–FR-008, SC-003, US1 scenarios 2 and 3).
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
  void aNamedTypeHasTheSchemaOfTheSetPlusTheConfiguredAnnotations(@Nonnull final String kind) {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    SchemaSetSources.write(kind, context, directory, database);

    final Dataset<Row> patients =
        SchemaSetSources.open(kind, withSchemas(Map.of(PATIENT, schema)), directory, database)
            .read(PATIENT);

    assertTrue(
        SchemaEquality.same(schema, patients.schema()),
        () -> "expected " + schema.treeString() + " but got " + patients.schema().treeString());
    assertEquals(
        annotationPaths(SchemaSetSources.expectedSchema(context, PATIENT, schema)),
        annotationPaths(patients.schema()));
    final Map<String, String> genders = byId(patients, "gender");
    final Map<String, String> birthDates = byId(patients, "birthDate");
    assertEquals(List.of("p1", "p2"), genders.keySet().stream().sorted().toList());
    assertEquals("female", genders.get("p1"));
    assertNull(genders.get("p2"));
    assertEquals("1990-01-01", birthDates.get("p2"));
    assertNull(birthDates.get("p1"));
  }

  /**
   * A type the set does not name is inferred, exactly as the same source infers it without a set,
   * while the named type beside it is projected (FR-010, US1 scenario 7).
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
  void aPartialSetProjectsTheNamedTypesAndInfersTheRest(@Nonnull final String kind) {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    SchemaSetSources.write(kind, context, directory, database);

    final QueryableDataSource defined =
        SchemaSetSources.open(kind, withSchemas(Map.of(PATIENT, schema)), directory, database);
    final QueryableDataSource inferred =
        SchemaSetSources.open(kind, context.read(), directory, database);

    assertEquals(inferred.read(CONDITION).schema(), defined.read(CONDITION).schema());
    assertTrue(SchemaEquality.same(schema, defined.read(PATIENT).schema()));
  }

  /**
   * A named type read from NDJSON or bundles is shaped once, by the io defined read: its plan is
   * that of the io defined read of the same files with the same schema, annotated, with no
   * projection on top (T019).
   */
  @ParameterizedTest
  @ValueSource(strings = {"ndjson", "bundlesJson", "bundlesXml"})
  void aTextReadIsShapedOnceByTheDefinedRead(@Nonnull final String kind) {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    SchemaSetSources.write(kind, context, directory, database);

    final Dataset<Row> patients =
        SchemaSetSources.open(kind, withSchemas(Map.of(PATIENT, schema)), directory, database)
            .read(PATIENT);

    final Dataset<Row> expected =
        context.annotate(definedRead(kind, patients.inputFiles(), schema), PATIENT);
    assertEquals(
        canonicalPlan(expected),
        canonicalPlan(patients),
        "expected the plan of the io defined read");
  }

  /**
   * Returns the canonical form of a dataset's analysed plan, as {@code sameResult} compares it, but
   * with the names of lambda variables normalised: each read names them from a counter, so two
   * reads of the same input never have the same names.
   */
  @Nonnull
  private static String canonicalPlan(@Nonnull final Dataset<Row> dataset) {
    return dataset
        .queryExecution()
        .analyzed()
        .canonicalized()
        .toString()
        .replaceAll("\\bx_\\d+", "x");
  }

  /**
   * A schema set is a map, and is accepted with more types than {@code Map.of} can take (FR-004,
   * FR-005).
   */
  @Test
  void aSchemaSetOfMoreThanTenTypesIsAccepted() {
    final StructType idOnly = new StructType().add("id", DataTypes.StringType);
    final Map<String, StructType> schemaSet = new HashMap<>();
    Stream.of(
            "Observation",
            "Encounter",
            "Procedure",
            "MedicationRequest",
            "AllergyIntolerance",
            "Immunization",
            "DiagnosticReport",
            "CarePlan",
            "Claim",
            "Organization",
            CONDITION)
        .forEach(type -> schemaSet.put(type, idOnly));
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    schemaSet.put(PATIENT, schema);
    SchemaSetSources.write(SchemaSetSources.NDJSON, context, directory, database);

    final QueryableDataSource source =
        SchemaSetSources.open(SchemaSetSources.NDJSON, withSchemas(schemaSet), directory, database);

    assertTrue(SchemaEquality.same(schema, source.read(PATIENT).schema()));
    assertTrue(SchemaEquality.same(idOnly, source.read(CONDITION).schema()));
  }

  /** The Parquet source's own schema option is gone, replaced by the schema set (FR-003). */
  @Test
  void parquetWithSchemasIsGone() {
    assertThrows(
        NoSuchMethodException.class,
        () -> DataSourceBuilder.class.getMethod("parquetWithSchemas", String.class, Map.class));
  }

  /**
   * A stored field whose name differs from the schema's only in case fails the read, naming the
   * resource type and both paths, rather than being read as a match or as an unrelated field (spec
   * edge case "A stored field whose name differs from the defined schema's only in case").
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "delta"})
  void aStoredFieldDifferingOnlyInCaseFailsTheRead(@Nonnull final String kind) {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    final Dataset<Row> stored =
        spark.sql(
            "SELECT 'Patient' AS resourceType, 'p1' AS id,"
                + " array(named_struct('Family', 'Smith')) AS name");
    store(kind, stored);
    final QueryableDataSource source = open(kind, Map.of(PATIENT, schema));

    final IllegalArgumentException failure =
        assertThrows(IllegalArgumentException.class, () -> source.read(PATIENT));

    assertNames(failure, PATIENT, "name.Family", "name.family");
  }

  /**
   * A struct holding two fields whose names differ only in case fails the read, naming the resource
   * type and both paths, rather than with Spark's ambiguous reference (spec edge case "A stored
   * field whose name differs from the defined schema's only in case").
   *
   * <p>The struct is held by a dataset, because neither Parquet nor Delta can give one to a source:
   * Delta refuses to store it, and Spark refuses to load a Parquet file holding it, top-level or
   * nested, with its own error naming one of the two columns, before the source sees the data.
   */
  @Test
  void aStoredStructWithFieldsDifferingOnlyInCaseFailsTheRead() {
    final StructType schema = SchemaSetSources.definedPatientSchema(context);
    final Dataset<Row> stored =
        spark.sql("SELECT 'Patient' AS resourceType, 'p1' AS id, 'p2' AS ID, 'male' AS gender");

    final IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                withSchemas(Map.of(PATIENT, schema))
                    .datasets()
                    .dataset(PATIENT, stored)
                    .read(PATIENT));

    assertNames(failure, PATIENT, "id", "ID");
  }

  /**
   * A struct holding two fields whose names differ only in case fails the read even where the
   * defined schema names neither of them, since a projection that descends into the struct cannot
   * tell which of the two is meant (spec edge case "A stored field whose name differs from the
   * defined schema's only in case"). The struct is held by a dataset, for the reason given on
   * {@link #aStoredStructWithFieldsDifferingOnlyInCaseFailsTheRead}.
   */
  @Test
  void aStoredStructWithFieldsDifferingOnlyInCaseFailsTheReadWhenTheSchemaNamesNeither() {
    final StructType schema =
        SchemaSetSources.reader(context)
            .json()
            .read(
                PATIENT,
                spark.createDataset(
                    List.of(
                        "{\"resourceType\":\"Patient\",\"id\":\"d\","
                            + "\"name\":[{\"given\":[\"Ann\"]}]}"),
                    Encoders.STRING()))
            .schema();
    final Dataset<Row> stored =
        spark.sql(
            "SELECT 'Patient' AS resourceType, 'p1' AS id,"
                + " array(named_struct('family', 'Smith', 'Family', 'Jones')) AS name");

    final IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                withSchemas(Map.of(PATIENT, schema))
                    .datasets()
                    .dataset(PATIENT, stored)
                    .read(PATIENT));

    assertNames(failure, PATIENT, "name.family", "name.Family");
  }

  /**
   * A stored source given a schema carrying annotation fields, under a configuration that enables
   * no annotations, has no annotations: the annotations of the given schema are not kept, but
   * recomputed as the configuration says (FR-007).
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "delta", "tables", "datasets"})
  void aStoredReadRecomputesTheAnnotationsOfTheGivenSchema(@Nonnull final String kind) {
    final StructType annotated =
        SchemaSetSources.expectedSchema(
            context, PATIENT, SchemaSetSources.definedPatientSchema(context));
    assertFalse(annotationPaths(annotated).isEmpty(), "the given schema should carry annotations");
    final PathlingContext unannotated =
        PathlingContext.builder(spark)
            .schemaConfiguration(
                SchemaConfiguration.builder()
                    .enableNumericAnnotation(false)
                    .enableRangeAnnotation(false)
                    .enableCanonicalAnnotation(false)
                    .build())
            .build();
    SchemaSetSources.write(kind, context, directory, database);

    final Dataset<Row> patients =
        SchemaSetSources.open(
                kind,
                unannotated.read().withSchemas(Map.of(PATIENT, annotated)),
                directory,
                database)
            .read(PATIENT);

    assertTrue(SchemaEquality.same(annotated, patients.schema()));
    assertEquals(List.of(), annotationPaths(patients.schema()));
  }

  /**
   * A schema set whose key is not a resource type, or whose schema is null, is refused when it is
   * given, naming the key (FR-004).
   */
  @Test
  void aSchemaSetWithAnUnknownTypeOrANullSchemaIsRefused() {
    final Map<String, StructType> unknown = Map.of("NotAResource", new StructType());
    final Map<String, StructType> nullSchema = new HashMap<>();
    nullSchema.put(PATIENT, null);

    final IllegalArgumentException unknownFailure =
        assertThrows(IllegalArgumentException.class, () -> withSchemas(unknown));
    final IllegalArgumentException nullFailure =
        assertThrows(IllegalArgumentException.class, () -> withSchemas(nullSchema));

    assertTrue(unknownFailure.getMessage().contains("NotAResource"), unknownFailure::getMessage);
    assertTrue(nullFailure.getMessage().contains(PATIENT), nullFailure::getMessage);
  }

  @Nonnull
  private static DataSourceBuilder withSchemas(@Nonnull final Map<String, StructType> schemaSet) {
    return context.read().withSchemas(schemaSet);
  }

  @Nonnull
  private QueryableDataSource open(
      @Nonnull final String kind, @Nonnull final Map<String, StructType> schemaSet) {
    return SchemaSetSources.open(kind, withSchemas(schemaSet), directory, database);
  }

  private void store(@Nonnull final String kind, @Nonnull final Dataset<Row> patients) {
    final String table = directory.resolve("Patient.parquet").toString();
    if (SchemaSetSources.DELTA.equals(kind)) {
      patients.write().format("delta").save(table);
    } else {
      patients.write().parquet(table);
    }
  }

  /**
   * Returns the io defined read of the files a text source read, as the source reads them: NDJSON
   * as one document per line, selecting the documents of the type, and bundles as one document per
   * file.
   */
  @Nonnull
  private static Dataset<Row> definedRead(
      @Nonnull final String kind, @Nonnull final String[] files, @Nonnull final StructType schema) {
    final boolean xml = SchemaSetSources.BUNDLES_XML.equals(kind);
    final FhirFormatReader reader =
        SchemaSetSources.reader(context)
            .format(xml ? PathlingContext.FHIR_XML : PathlingContext.FHIR_JSON);
    if (SchemaSetSources.NDJSON.equals(kind)) {
      final Dataset<String> lines = spark.read().format("text").load(files).as(Encoders.STRING());
      final Column type =
          functions.get_json_object(lines.col(lines.columns()[0]), "$.resourceType");
      return reader.read(PATIENT, lines.filter(type.isNull().or(type.equalTo(PATIENT))), schema);
    }
    return reader.readBundles(
        PATIENT,
        spark.read().option("wholetext", true).format("text").load(files).as(Encoders.STRING()),
        schema);
  }

  /**
   * Asserts that a failure names a resource type, and each of a number of paths delimited by
   * backticks, so that a path is not matched inside a longer name.
   */
  private static void assertNames(
      @Nonnull final IllegalArgumentException failure,
      @Nonnull final String resourceType,
      @Nonnull final String... paths) {
    final String message = failure.getMessage();
    assertTrue(
        message.contains(resourceType), () -> "should name " + resourceType + ": " + message);
    Arrays.stream(paths)
        .map(path -> "`" + path + "`")
        .forEach(
            path ->
                assertTrue(message.contains(path), () -> "should name " + path + ": " + message));
  }
}

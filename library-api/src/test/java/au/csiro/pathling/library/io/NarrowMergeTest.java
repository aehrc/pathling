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

package au.csiro.pathling.library.io;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that merging into a Delta table through the library API accepts a source whose schema
 * differs from the target's in either direction. A source narrower than the target leaves the
 * target's additional columns null on the written rows; a source wider than the target widens it,
 * as an append does (decision 41, T117a).
 *
 * <p>Under a schema fitted to the data, two batches of the same resource type differ whenever their
 * resources populate different elements, so both directions are ordinary use. The resources are
 * encoded through {@link PathlingContext#encode}, so the divergence comes from the data rather than
 * from encoder configuration. {@code DeltaSchemaMergingTest} covers a source differing in both
 * directions at the top level and inside a repeating element; this class covers each direction on
 * its own, and the choice value of an extension, which is where a narrower encoder configuration
 * used to differ.
 *
 * @author John Grimes
 */
class NarrowMergeTest {

  private static final String STRING_EXTENSION_URL = "http://example.org/string-extension";
  private static final String PERIOD_EXTENSION_URL = "http://example.org/period-extension";
  private static final String TABLE_FILE_NAME = "Patient.parquet";

  private static SparkSession spark;
  private static Path temporaryDirectory;
  private static PathlingContext context;
  private static IParser parser;

  /** Set up Spark and a temporary warehouse. */
  @BeforeAll
  static void setUp() throws IOException {
    temporaryDirectory = Files.createTempDirectory("pathling-narrow-merge-test-");
    spark = TestHelpers.sparkBuilder().getOrCreate();
    context = PathlingContext.create(spark);
    parser = context.getFhirContext().newJsonParser();
  }

  /** Tear down Spark and remove the temporary warehouse. */
  @AfterAll
  static void tearDown() throws IOException {
    spark.stop();
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  /**
   * Merging a source that lacks a column the target carries succeeds, leaves that column null on
   * the written rows, and replaces the row whose id already existed (US3.1, US3.2, SC-007).
   */
  @Test
  void narrowerSourceMergesAndLeavesTheMissingColumnNull() {
    // Arrange: a target holding two patients, both with a gender.
    final String warehouse = warehouse("narrower-source");
    write(encode(targetPatients()), warehouse, "overwrite");

    // Arrange: a source that updates one of them and adds a third, carrying no gender column at
    // all.
    final Dataset<Row> narrowerSource = encode(sourcePatients()).drop("gender");

    // Act.
    write(narrowerSource, warehouse, "merge");

    // Assert: the untouched row keeps its gender, and the two written rows have none.
    final Map<String, String> genders = valuesById(warehouse, "gender");
    assertEquals(3, genders.size());
    assertEquals(AdministrativeGender.MALE.toCode(), genders.get("target-only"));
    assertNull(genders.get("in-both"));
    assertNull(genders.get("source-only"));

    // Assert: the matched row was replaced, not merged field by field.
    assertEquals(
        "the updated value",
        stringExtensionValueOf(warehouse, "in-both"),
        "the row matching an existing id should have been replaced");
  }

  /** The target keeps its own schema after a narrower source is merged into it (US3.3). */
  @Test
  void mergeLeavesTheTargetSchemaUnchanged() {
    // Arrange.
    final String warehouse = warehouse("schema-unchanged");
    write(encode(targetPatients()), warehouse, "overwrite");
    final StructType before = storedSchema(warehouse);

    // Act.
    write(encode(sourcePatients()).drop("gender"), warehouse, "merge");

    // Assert: the schema is exactly what it was, so the merge widened nothing and dropped nothing.
    assertEquals(before, storedSchema(warehouse));
  }

  /**
   * A source whose extensions carry fewer choice values than the target's merges into it, and the
   * rows written before it keep their values (US3.1, SC-007).
   *
   * <p>This is the state the reporter of #2697 described, at the level of nested struct fields
   * rather than whole columns: the target's extension struct carries {@code valuePeriod}, and the
   * source's does not.
   */
  @Test
  void narrowerExtensionSourceMergesIntoAWiderTable() {
    // Arrange: a target whose extensions carry a period as well as a string.
    final String warehouse = warehouse("narrower-extension");
    write(encode(withPeriodExtensions(targetPatients())), warehouse, "overwrite");
    final StructType before = storedSchema(warehouse);

    // Act: merge a source whose extensions carry only a string.
    write(encode(sourcePatients()), warehouse, "merge");

    // Assert: the table kept its wider schema.
    assertEquals(before, storedSchema(warehouse));

    // Assert: the row written before the merge and never touched still carries its values.
    assertEquals(
        "the original value",
        stringExtensionValueOf(warehouse, "target-only"),
        "a row written before the merge should still carry its value");
    assertEquals("2020-01-01", periodExtensionStartOf(warehouse, "target-only"));
    // Assert: and the newly written rows are there.
    assertEquals(3, valuesById(warehouse, "gender").size());
  }

  /**
   * A source whose extensions carry a choice value the target's do not widens the target inside the
   * extension structure, rather than being refused as it was under the previous layout (decision
   * 41, T117a).
   *
   * <p>This is the inverse of {@link #narrowerExtensionSourceMergesIntoAWiderTable}.
   */
  @Test
  void widerExtensionSourceWidensTheTarget() {
    // Arrange: a target whose extensions carry only a string.
    final String warehouse = warehouse("wider-extension");
    write(encode(targetPatients()), warehouse, "overwrite");
    assertFalse(extensionFields(storedSchema(warehouse)).contains("valuePeriod"));

    // Act: merge a source whose extensions carry a period as well.
    write(encode(withPeriodExtensions(sourcePatients())), warehouse, "merge");

    // Assert: the target's extension structure gained the period.
    assertTrue(extensionFields(storedSchema(warehouse)).contains("valuePeriod"));

    // Assert: the written rows carry it, the matched row having been replaced, and the untouched
    // row has none.
    assertEquals(3, valuesById(warehouse, "gender").size());
    assertEquals("2020-01-01", periodExtensionStartOf(warehouse, "in-both"));
    assertEquals("2020-01-01", periodExtensionStartOf(warehouse, "source-only"));
    assertNull(periodExtensionStartOf(warehouse, "target-only"));
    assertEquals("the updated value", stringExtensionValueOf(warehouse, "in-both"));
    assertEquals("the original value", stringExtensionValueOf(warehouse, "target-only"));
  }

  /**
   * A source carrying a top-level element the target lacks widens the target with that column,
   * leaving it null on the rows the source did not write (decision 41, T117a).
   */
  @Test
  void widerTopLevelSourceWidensTheTarget() {
    // Arrange.
    final String warehouse = warehouse("wider-column");
    write(encode(targetPatients()), warehouse, "overwrite");
    assertFalse(List.of(storedSchema(warehouse).fieldNames()).contains("birthDate"));

    // Act.
    write(encode(withBirthDates(sourcePatients())), warehouse, "merge");

    // Assert: the target gained the column, and holds the source's values in it.
    assertTrue(List.of(storedSchema(warehouse).fieldNames()).contains("birthDate"));
    final Map<String, String> birthDates = valuesById(warehouse, "birthDate");
    assertEquals(3, birthDates.size());
    assertEquals("1990-01-01", birthDates.get("in-both"));
    assertEquals("1990-01-01", birthDates.get("source-only"));
    assertNull(birthDates.get("target-only"));
  }

  /**
   * A source that differs from the target in both directions at once merges, widening the target
   * with what it carries and leaving null what it lacks on the rows it wrote (decision 41, T117a).
   */
  @Test
  void sourceDifferingInBothDirectionsWidensTheTarget() {
    // Arrange: a source that both lacks a column the target has and carries one it does not.
    final String warehouse = warehouse("both-directions");
    write(encode(targetPatients()), warehouse, "overwrite");
    final Dataset<Row> mixedSource = encode(withBirthDates(sourcePatients())).drop("gender");

    // Act.
    write(mixedSource, warehouse, "merge");

    // Assert: the target keeps its own columns and gains the source's.
    final List<String> fields = List.of(storedSchema(warehouse).fieldNames());
    assertTrue(fields.contains("gender"), "the target should still carry its own columns");
    assertTrue(fields.contains("birthDate"), "the target should have been widened by the source");

    // Assert: the matched row was replaced in full, and the untouched row kept its values.
    final Map<String, String> genders = valuesById(warehouse, "gender");
    assertEquals(3, genders.size());
    assertEquals(AdministrativeGender.MALE.toCode(), genders.get("target-only"));
    assertNull(genders.get("in-both"));
    final Map<String, String> birthDates = valuesById(warehouse, "birthDate");
    assertEquals("1990-01-01", birthDates.get("in-both"));
    assertNull(birthDates.get("target-only"));
  }

  /**
   * A source carrying a column the definitions do not describe widens the target with it, as an
   * append does, rather than ignoring it as the merge did before it widened (decision 41, T117a).
   */
  @Test
  void sourceWithAnUndescribedColumnWidensTheTarget() {
    // Arrange.
    final String warehouse = warehouse("undescribed-column");
    write(encode(targetPatients()), warehouse, "overwrite");
    final Dataset<Row> widerSource = encode(sourcePatients()).withColumn("unexpected", lit(1));

    // Act.
    write(widerSource, warehouse, "merge");

    // Assert: the target gained the column, which holds the source's value on the rows it wrote
    // and is null on the row it did not.
    assertTrue(List.of(storedSchema(warehouse).fieldNames()).contains("unexpected"));
    final Map<String, String> unexpected = valuesById(warehouse, "unexpected");
    assertEquals(3, unexpected.size());
    assertEquals("1", unexpected.get("in-both"));
    assertEquals("1", unexpected.get("source-only"));
    assertNull(unexpected.get("target-only"));
    assertEquals(
        AdministrativeGender.MALE.toCode(), valuesById(warehouse, "gender").get("target-only"));
  }

  /** A merge whose source and target schemas are aligned behaves as it does today (US3.7). */
  @Test
  void alignedMergeIsUnaffected() {
    // Arrange.
    final String warehouse = warehouse("aligned");
    write(encode(targetPatients()), warehouse, "overwrite");

    // Act.
    assertDoesNotThrow(() -> write(encode(sourcePatients()), warehouse, "merge"));

    // Assert: three rows, with the source's own genders intact rather than null-filled.
    final Map<String, String> genders = valuesById(warehouse, "gender");
    assertEquals(3, genders.size());
    assertEquals(AdministrativeGender.MALE.toCode(), genders.get("target-only"));
    assertEquals(AdministrativeGender.FEMALE.toCode(), genders.get("in-both"));
    assertEquals(AdministrativeGender.FEMALE.toCode(), genders.get("source-only"));
  }

  // Fixtures and helpers.

  /** Two patients, one of which the source also carries. */
  @Nonnull
  private static List<Patient> targetPatients() {
    return List.of(
        patient("target-only", AdministrativeGender.MALE, "the original value"),
        patient("in-both", AdministrativeGender.MALE, "the original value"));
  }

  /** One patient the target already holds, and one it does not. */
  @Nonnull
  private static List<Patient> sourcePatients() {
    return List.of(
        patient("in-both", AdministrativeGender.FEMALE, "the updated value"),
        patient("source-only", AdministrativeGender.FEMALE, "the updated value"));
  }

  @Nonnull
  private static Patient patient(
      @Nonnull final String id,
      @Nonnull final AdministrativeGender gender,
      @Nonnull final String extensionValue) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.setGender(gender);
    patient.addExtension(new Extension(STRING_EXTENSION_URL, new StringType(extensionValue)));
    return patient;
  }

  /** Adds an extension holding a period to each patient. */
  @Nonnull
  private static List<Patient> withPeriodExtensions(@Nonnull final List<Patient> patients) {
    patients.forEach(
        patient ->
            patient.addExtension(
                new Extension(
                    PERIOD_EXTENSION_URL,
                    new Period().setStartElement(new DateTimeType("2020-01-01")))));
    return patients;
  }

  /** Gives each patient a birth date. */
  @Nonnull
  private static List<Patient> withBirthDates(@Nonnull final List<Patient> patients) {
    patients.forEach(patient -> patient.setBirthDateElement(new DateType("1990-01-01")));
    return patients;
  }

  /** Encodes the patients through the library API. */
  @Nonnull
  private static Dataset<Row> encode(@Nonnull final List<Patient> patients) {
    final List<String> json = patients.stream().map(parser::encodeResourceToString).toList();
    return context.encode(spark.createDataset(json, Encoders.STRING()).toDF(), "Patient");
  }

  @Nonnull
  private static String warehouse(@Nonnull final String name) {
    return temporaryDirectory.resolve(name).toString();
  }

  private static void write(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String warehouse,
      @Nonnull final String saveMode) {
    context
        .read()
        .datasets()
        .dataset("Patient", dataset)
        .write()
        .saveMode(saveMode)
        .delta(warehouse);
  }

  @Nonnull
  private static StructType storedSchema(@Nonnull final String warehouse) {
    return storedTable(warehouse).schema();
  }

  @Nonnull
  private static Dataset<Row> storedTable(@Nonnull final String warehouse) {
    return spark.read().format("delta").load(Path.of(warehouse, TABLE_FILE_NAME).toString());
  }

  /** Returns the field names of the struct inside the {@code extension} array of a schema. */
  @Nonnull
  private static List<String> extensionFields(@Nonnull final StructType schema) {
    final StructType extension =
        (StructType) ((ArrayType) schema.apply("extension").dataType()).elementType();
    return Arrays.asList(extension.fieldNames());
  }

  /** Returns the value of one column held against each stored id, which is null where none was. */
  @Nonnull
  private static Map<String, String> valuesById(
      @Nonnull final String warehouse, @Nonnull final String column) {
    final Map<String, String> values = new HashMap<>();
    storedTable(warehouse)
        .selectExpr("id", "CAST(" + column + " AS STRING)")
        .collectAsList()
        .forEach(row -> values.put(row.getString(0), row.getString(1)));
    return values;
  }

  /** Returns the string extension value of the stored resource with the given id. */
  @Nullable
  private static String stringExtensionValueOf(
      @Nonnull final String warehouse, @Nonnull final String id) {
    return extensionValue(warehouse, id, STRING_EXTENSION_URL, "valueString");
  }

  /** Returns the start of the period extension of the stored resource with the given id. */
  @Nullable
  private static String periodExtensionStartOf(
      @Nonnull final String warehouse, @Nonnull final String id) {
    return extensionValue(warehouse, id, PERIOD_EXTENSION_URL, "valuePeriod.start");
  }

  /**
   * Returns a value of the extension with the given URL on the resource with the given id, read
   * back through the library API.
   */
  @Nullable
  private static String extensionValue(
      @Nonnull final String warehouse,
      @Nonnull final String id,
      @Nonnull final String url,
      @Nonnull final String field) {
    final List<String> values =
        context
            .read()
            .delta(warehouse)
            .read("Patient")
            .filter("id = '" + id + "'")
            .selectExpr("explode(extension) AS e")
            .filter("e.url = '" + url + "'")
            .selectExpr("CAST(e." + field + " AS STRING)")
            .as(Encoders.STRING())
            .collectAsList();
    return values.isEmpty() ? null : values.getFirst();
  }
}

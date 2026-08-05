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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that merging into a Delta table through the library API tolerates a source narrower than
 * the target, leaving the target's additional columns null on the written rows, while continuing to
 * reject a source that would widen the target.
 *
 * <p>Both directions matter. The tolerance is what makes a warehouse written by one encoder
 * configuration writable by a narrower one; refusing to widen is what keeps the tolerance from
 * quietly turning into schema evolution the caller did not ask for.
 *
 * @author John Grimes
 */
class NarrowMergeTest {

  private static final String STRING_EXTENSION_URL = "http://example.org/string-extension";
  private static final String TABLE_FILE_NAME = "Patient.parquet";

  private static SparkSession spark;
  private static Path temporaryDirectory;
  private static PathlingContext wideContext;
  private static PathlingContext narrowContext;

  /** Set up Spark and a temporary warehouse. */
  @BeforeAll
  static void setUp() throws IOException {
    temporaryDirectory = Files.createTempDirectory("pathling-narrow-merge-test-");
    spark = TestHelpers.sparkBuilder().getOrCreate();
    wideContext = ExtensionContexts.wide(spark);
    narrowContext = ExtensionContexts.narrow(spark);
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
   *
   * <p>Before the fix this failed with {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION}.
   */
  @Test
  void narrowerSourceMergesAndLeavesTheMissingColumnNull() {
    // Arrange: a target holding two patients, both with a gender.
    final String warehouse = warehouse("narrower-source");
    write(
        wideContext, ExtensionContexts.encodeWide(spark, targetPatients()), warehouse, "overwrite");

    // Arrange: a source that updates one of them and adds a third, carrying no gender column at
    // all.
    final Dataset<Row> narrowerSource =
        ExtensionContexts.encodeWide(spark, sourcePatients()).drop("gender");

    // Act.
    write(wideContext, narrowerSource, warehouse, "merge");

    // Assert: the untouched row keeps its gender, and the two written rows have none.
    final Map<String, String> genders = gendersById(warehouse);
    assertEquals(3, genders.size());
    assertEquals(AdministrativeGender.MALE.toCode(), genders.get("target-only"));
    assertNull(genders.get("in-both"));
    assertNull(genders.get("source-only"));

    // Assert: the matched row was replaced, not merged field by field.
    assertEquals(
        "the updated value",
        extensionValueOf(warehouse, "in-both"),
        "the row matching an existing id should have been replaced");
  }

  /**
   * The target keeps its own schema after a narrower source is merged into it, so a reader
   * configured with the original open types still reads the columns written before (US3.3).
   */
  @Test
  void mergeLeavesTheTargetSchemaUnchanged() {
    // Arrange.
    final String warehouse = warehouse("schema-unchanged");
    write(
        wideContext, ExtensionContexts.encodeWide(spark, targetPatients()), warehouse, "overwrite");
    final StructType before = storedSchema(warehouse);

    // Act.
    write(
        wideContext,
        ExtensionContexts.encodeWide(spark, sourcePatients()).drop("gender"),
        warehouse,
        "merge");

    // Assert: the schema is exactly what it was, so the merge widened nothing and dropped nothing.
    assertEquals(before, storedSchema(warehouse));
  }

  /**
   * A narrowly-encoded source merges into a table written with more open types configured, and the
   * rows written before it remain readable by the encoder that wrote them (US3.1, SC-007).
   *
   * <p>This is the state the reporter of #2697 described, at the level of nested struct fields
   * rather than whole columns: the target's extension struct carries {@code valuePeriod} and {@code
   * valueQuantity}, and the source's does not.
   */
  @Test
  void narrowlyEncodedSourceMergesIntoAWiderTable() {
    // Arrange: a target written with Period and Quantity configured.
    final String warehouse = warehouse("narrow-encoder");
    write(
        wideContext, ExtensionContexts.encodeWide(spark, targetPatients()), warehouse, "overwrite");
    final StructType before = storedSchema(warehouse);

    // Act: merge a source encoded without them, through the narrow context.
    write(
        narrowContext, ExtensionContexts.encodeNarrow(spark, sourcePatients()), warehouse, "merge");

    // Assert: the table kept its wider schema.
    assertEquals(before, storedSchema(warehouse));

    // Assert: the row written by the wide encoder and never touched still decodes to its original
    // extension value, read back with the wide encoder through the library API.
    assertEquals(
        "the original value",
        extensionValueOf(warehouse, "target-only"),
        "a row written before the merge should still carry its value");
    // Assert: and the newly written rows are there.
    assertEquals(3, gendersById(warehouse).size());
  }

  /**
   * A source whose nested structs carry fields the target's do not still fails, so the change adds
   * tolerance rather than schema evolution (US3.4, FR-012).
   *
   * <p>This is the inverse of {@link #narrowlyEncodedSourceMergesIntoAWiderTable}: a widely-encoded
   * source merged into a narrowly-encoded target.
   */
  @Test
  void wideningSourceStillFails() {
    // Arrange: a target written with the narrow encoder.
    final String warehouse = warehouse("widening-source");
    write(
        narrowContext,
        ExtensionContexts.encodeNarrow(spark, targetPatients()),
        warehouse,
        "overwrite");
    final StructType before = storedSchema(warehouse);

    // Act and assert: merging a widely-encoded source is refused, for the same reason it is today.
    final AnalysisException failure =
        assertThrows(
            AnalysisException.class,
            () ->
                write(
                    wideContext,
                    ExtensionContexts.encodeWide(spark, sourcePatients()),
                    warehouse,
                    "merge"));
    assertTrue(
        failure.getMessage().contains("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION"),
        "expected the merge to fail on the struct mismatch, but got: " + failure.getMessage());

    // Assert: the target was left alone, and in particular was not widened.
    assertEquals(2, gendersById(warehouse).size());
    assertEquals(before, storedSchema(warehouse));
  }

  /**
   * A source carrying a top-level column the target lacks merges as it does today, ignoring that
   * column, and does not widen the target (US3.4, FR-012).
   *
   * <p>This shape of widening difference does not fail today, unlike the nested one: {@code
   * updateAll} and {@code insertAll} address the target's columns, so a column only the source
   * carries is simply not written. What matters for FR-012 is that enabling the tolerance for a
   * narrower source has not turned this into schema evolution.
   */
  @Test
  void wideningSourceDoesNotWidenTheTarget() {
    // Arrange.
    final String warehouse = warehouse("widening-column");
    write(
        wideContext, ExtensionContexts.encodeWide(spark, targetPatients()), warehouse, "overwrite");
    final StructType before = storedSchema(warehouse);
    final Dataset<Row> widerSource =
        ExtensionContexts.encodeWide(spark, sourcePatients()).withColumn("unexpected", lit(1));

    // Act.
    write(wideContext, widerSource, warehouse, "merge");

    // Assert: the rows were written, and the target's schema is untouched.
    assertEquals(3, gendersById(warehouse).size());
    assertEquals(before, storedSchema(warehouse));
    assertTrue(
        !List.of(storedSchema(warehouse).fieldNames()).contains("unexpected"),
        "the target should not have been widened by the source");
  }

  /**
   * A source that differs from the target in both directions at once still fails, because admitting
   * it would widen the target (US3.5, FR-012).
   */
  @Test
  void sourceDifferingInBothDirectionsStillFails() {
    // Arrange: a source that both lacks a column the target has and carries one it does not.
    final String warehouse = warehouse("both-directions");
    write(
        wideContext, ExtensionContexts.encodeWide(spark, targetPatients()), warehouse, "overwrite");
    final Dataset<Row> mixedSource =
        ExtensionContexts.encodeWide(spark, sourcePatients())
            .drop("gender")
            .withColumn("unexpected", lit(1));

    // Act and assert: refused, for the same reason it is today - the column the source lacks cannot
    // be resolved in the update clause.
    final AnalysisException failure =
        assertThrows(
            AnalysisException.class, () -> write(wideContext, mixedSource, warehouse, "merge"));
    assertTrue(
        failure.getMessage().contains("DELTA_MERGE_UNRESOLVED_EXPRESSION"),
        "expected the merge to fail on the unresolved column, but got: " + failure.getMessage());

    // Assert: the target was left alone, and in particular was not widened.
    assertEquals(2, gendersById(warehouse).size());
    assertTrue(
        List.of(storedSchema(warehouse).fieldNames()).contains("gender"),
        "the target should still carry its own columns");
    assertTrue(
        !List.of(storedSchema(warehouse).fieldNames()).contains("unexpected"),
        "the target should not have been widened by the source");
  }

  /** A merge whose source and target schemas are aligned behaves as it does today (US3.7). */
  @Test
  void alignedMergeIsUnaffected() {
    // Arrange.
    final String warehouse = warehouse("aligned");
    write(
        wideContext, ExtensionContexts.encodeWide(spark, targetPatients()), warehouse, "overwrite");

    // Act.
    assertDoesNotThrow(
        () ->
            write(
                wideContext,
                ExtensionContexts.encodeWide(spark, sourcePatients()),
                warehouse,
                "merge"));

    // Assert: three rows, with the source's own genders intact rather than null-filled.
    final Map<String, String> genders = gendersById(warehouse);
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

  @Nonnull
  private static String warehouse(@Nonnull final String name) {
    return temporaryDirectory.resolve(name).toString();
  }

  private static void write(
      @Nonnull final PathlingContext context,
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

  /** Returns the gender code held against each stored id, which is null where none was written. */
  @Nonnull
  private static Map<String, String> gendersById(@Nonnull final String warehouse) {
    final Map<String, String> genders = new HashMap<>();
    storedTable(warehouse)
        .select("id", "gender")
        .collectAsList()
        .forEach(row -> genders.put(row.getString(0), row.getString(1)));
    return genders;
  }

  /**
   * Returns the string extension value of the stored resource with the given id, decoded through
   * the library API with the wide encoder.
   */
  @Nonnull
  private static String extensionValueOf(
      @Nonnull final String warehouse, @Nonnull final String id) {
    final Dataset<Row> row =
        wideContext.read().delta(warehouse).read("Patient").filter("id = '" + id + "'");
    final Patient decoded = ExtensionContexts.decodeOne(wideContext, row);
    return ((StringType) decoded.getExtensionByUrl(STRING_EXTENSION_URL).getValue()).getValue();
  }
}

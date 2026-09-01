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

import static au.csiro.pathling.encoders.utils.SchemaMisalignment.elementStruct;
import static au.csiro.pathling.encoders.utils.SchemaMisalignment.wideEncoders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The Delta-level form of the decode regression: a table created with a narrower encoder, migrated
 * in place by a zero-row {@code mergeSchema} append using a wider one, then read back and decoded
 * through the library API.
 *
 * <p>This is the reproduction recorded against #2698. The projection-based version of it lives in
 * the {@code encoders} module, which has no Delta dependency; this one exercises a real migrated
 * table, so it also confirms that {@code mergeSchema} appends new nested fields to the end of each
 * struct rather than merging them into the encoder's order.
 *
 * @author John Grimes
 */
class MigratedTableDecodingTest {

  private static final String STRING_EXTENSION_URL = "http://example.org/string-extension";
  private static final String INTEGER_EXTENSION_URL = "http://example.org/integer-extension";

  private static SparkSession spark;
  private static Path temporaryDirectory;

  /** Set up Spark and a temporary warehouse. */
  @BeforeAll
  static void setUp() throws IOException {
    temporaryDirectory = Files.createTempDirectory("pathling-migrated-table-test-");
    spark = TestHelpers.sparkBuilder().getOrCreate();
  }

  /** Tear down Spark and remove the temporary warehouse. */
  @AfterAll
  static void tearDown() throws IOException {
    spark.stop();
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  /**
   * A Delta table created with the narrow encoder and then migrated in place by a zero-row {@code
   * mergeSchema} append using the wide encoder returns, when read back and decoded through the
   * library API, the extension values it was written with (US1.4, SC-006).
   */
  @Test
  void migratedDeltaTableDecodesToTheValuesWritten() {
    final PathlingContext narrowContext = ExtensionContexts.narrow(spark);
    final PathlingContext wideContext = ExtensionContexts.wide(spark);
    final String warehouse = temporaryDirectory.resolve("migrated-warehouse").toString();
    final String tablePath = Path.of(warehouse, "Patient.parquet").toString();

    // Arrange: create the table with the narrow encoder, through the library API sink.
    narrowContext
        .read()
        .datasets()
        .dataset("Patient", ExtensionContexts.encodeNarrow(spark, List.of(patientWithExtensions())))
        .write()
        .saveMode("overwrite")
        .delta(warehouse);

    // Arrange: migrate the table in place, as a schemaAutoMerge warmup write does - a zero-row
    // append by the wide encoder with mergeSchema enabled.
    ExtensionContexts.encodeWide(spark, Collections.emptyList())
        .write()
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(tablePath);

    // Guard: the migration must have left the stored extension struct in a different field order
    // from the one the wide encoder emits, or the test proves nothing.
    final StructType storedSchema = spark.read().format("delta").load(tablePath).schema();
    assertFalse(
        Arrays.equals(
            extensionFieldNames(storedSchema),
            extensionFieldNames(wideEncoders().of(Patient.class).schema())),
        "the migrated table should not carry the wide encoder's field order");

    // Act: read the table back and decode it through the library API, using the wide encoder that
    // performed the migration.
    final Patient decoded =
        ExtensionContexts.decodeOne(
            wideContext, wideContext.read().delta(warehouse).read("Patient"));

    // Assert: the extension values are the ones that were written.
    assertEquals(
        "the written value",
        ((StringType) decoded.getExtensionByUrl(STRING_EXTENSION_URL).getValue()).getValue());
    assertEquals(
        42, ((IntegerType) decoded.getExtensionByUrl(INTEGER_EXTENSION_URL).getValue()).getValue());
    // Assert: and the narrow encoder that wrote the table can still read it too.
    assertEquals(
        "the written value",
        ((StringType)
                ExtensionContexts.decodeOne(
                        narrowContext, narrowContext.read().delta(warehouse).read("Patient"))
                    .getExtensionByUrl(STRING_EXTENSION_URL)
                    .getValue())
            .getValue());
  }

  private static Patient patientWithExtensions() {
    final Patient patient = new Patient();
    patient.setId("migrated-patient");
    patient.addExtension(new Extension(STRING_EXTENSION_URL, new StringType("the written value")));
    patient.addExtension(new Extension(INTEGER_EXTENSION_URL, new IntegerType(42)));
    return patient;
  }

  /** Returns the field names of the struct held in the extension container's arrays. */
  private static String[] extensionFieldNames(final StructType schema) {
    return elementStruct(schema, ExtensionSupport.EXTENSIONS_FIELD_NAME()).fieldNames();
  }
}

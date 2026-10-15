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
package au.csiro.pathling.legacy;

import static au.csiro.pathling.legacy.LegacyTables.LIBRARY_DEFAULTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the command-line contract of the export: a source, a format and an output, and a non-zero
 * exit with a clear message where the source is not legacy or cannot be read (FR-064).
 */
class LegacyExportCommandTest {

  @Nonnull private static final String PATIENTS = "/data/synthea/R4/Patient.ndjson";

  @Nonnull private static final String CONDITIONS = "/data/synthea/R4/Condition.ndjson";

  @TempDir Path temp;

  private ByteArrayOutputStream out;

  private ByteArrayOutputStream err;

  @BeforeEach
  void setUp() {
    // The command takes the session that is already active, so the tests share theirs with it.
    LegacyTables.spark();
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
  }

  @Test
  void exportsAParquetTable() {
    final String table = legacyParquet("Patient", PATIENTS, temp.resolve("Patient.parquet"));
    final Path output = temp.resolve("out");

    final int status = run(table, "parquet", output.toString());

    assertEquals(0, status, "stderr: " + err);
    assertEquals(
        LegacyTables.lines(PATIENTS).size(), LegacyTables.exported(output, "Patient").size());
    assertTrue(out().contains("Patient"), "the command reports what it exported: " + out());
  }

  @Test
  void exportsADeltaTable() {
    final String table =
        LegacyTables.writeDelta(
            LegacyTables.encode(LIBRARY_DEFAULTS, "Patient", LegacyTables.lines(PATIENTS)),
            temp.resolve("Patient.parquet"));
    final Path output = temp.resolve("out");

    final int status = run(table, "delta", output.toString());

    assertEquals(0, status, "stderr: " + err);
    assertEquals(
        LegacyTables.lines(PATIENTS).size(), LegacyTables.exported(output, "Patient").size());
  }

  @Test
  void exportsEveryTableOfADirectory() {
    final Path warehouse = temp.resolve("warehouse");
    legacyParquet("Patient", PATIENTS, warehouse.resolve("Patient.parquet"));
    legacyParquet("Condition", CONDITIONS, warehouse.resolve("Condition.parquet"));
    final Path output = temp.resolve("out");

    final int status = run(warehouse.toString(), "parquet", output.toString());

    assertEquals(0, status, "stderr: " + err);
    assertEquals(
        LegacyTables.lines(PATIENTS).size(), LegacyTables.exported(output, "Patient").size());
    assertEquals(
        LegacyTables.lines(CONDITIONS).size(), LegacyTables.exported(output, "Condition").size());
  }

  @Test
  void takesTheResourceTypeFromAnOption() {
    final String table = legacyParquet("Patient", PATIENTS, temp.resolve("people"));
    final Path output = temp.resolve("out");

    final int status = run(table, "parquet", output.toString(), "--resource-type", "Patient");

    assertEquals(0, status, "stderr: " + err);
    assertEquals(
        LegacyTables.lines(PATIENTS).size(), LegacyTables.exported(output, "Patient").size());
  }

  @Test
  void failsOnATableNotInTheLegacyLayout() {
    final Path table = temp.resolve("Patient.parquet");
    LegacyTables.spark()
        .read()
        .json(LegacyTables.spark().createDataset(LegacyTables.lines(PATIENTS), Encoders.STRING()))
        .write()
        .parquet(table.toString());

    final int status = run(table.toString(), "parquet", temp.resolve("out").toString());

    assertNotEquals(0, status);
    assertTrue(err().contains("not in the legacy layout"), "stderr: " + err());
  }

  @Test
  void failsOnAnUnreadableSource() {
    final String missing = temp.resolve("Patient.parquet").toString();

    final int status = run(missing, "parquet", temp.resolve("out").toString());

    assertNotEquals(0, status);
    assertTrue(err().contains(missing), "stderr: " + err());
  }

  @Test
  void failsOnASourceThatNamesNoResourceType() {
    final String table = legacyParquet("Patient", PATIENTS, temp.resolve("people"));

    final int status = run(table, "parquet", temp.resolve("out").toString());

    assertNotEquals(0, status);
    assertTrue(err().contains("--resource-type"), "stderr: " + err());
  }

  @Test
  void failsOnAnUnknownFormat() {
    final int status =
        run(temp.resolve("Patient.parquet").toString(), "csv", temp.resolve("out").toString());

    assertNotEquals(0, status);
    assertTrue(err().contains("csv"), "stderr: " + err());
    assertTrue(err().contains("parquet") && err().contains("delta"), "stderr: " + err());
  }

  @Test
  void failsWithUsageOnMissingArguments() {
    final int status = run("only-a-source");

    assertNotEquals(0, status);
    assertTrue(err().contains("Usage"), "stderr: " + err());
  }

  @Test
  void acceptsTheFormatInAnyCase() {
    final String table = legacyParquet("Patient", PATIENTS, temp.resolve("Patient.parquet"));
    final Path output = temp.resolve("out");

    final int status = run(table, "PARQUET", output.toString());

    assertEquals(0, status, "stderr: " + err);
  }

  @Test
  void failsOnADirectoryHoldingTwoTablesOfOneResourceType() {
    final Path warehouse = temp.resolve("warehouse");
    legacyParquet("Patient", PATIENTS, warehouse.resolve("Patient.parquet"));
    legacyParquet("Patient", PATIENTS, warehouse.resolve("Patient.old.parquet"));

    final int status = run(warehouse.toString(), "parquet", temp.resolve("out").toString());

    assertNotEquals(0, status);
    assertTrue(err().contains("more than one table of Patient"), "stderr: " + err());
  }

  @Test
  void failsWithUsageOnAnUnknownOption() {
    final int status = run("Patient.parquet", "parquet", "out", "--verbose");

    assertNotEquals(0, status);
    assertTrue(err().contains("--verbose"), "stderr: " + err());
    assertTrue(err().contains("Usage"), "stderr: " + err());
  }

  @Test
  void failsWithUsageOnAResourceTypeOptionWithoutAValue() {
    final int status = run("people", "parquet", "out", "--resource-type");

    assertNotEquals(0, status);
    assertTrue(err().contains("--resource-type needs a resource type"), "stderr: " + err());
  }

  private int run(@Nonnull final String... args) {
    return LegacyExportCommand.run(
        args,
        new PrintStream(out, true, StandardCharsets.UTF_8),
        new PrintStream(err, true, StandardCharsets.UTF_8));
  }

  @Nonnull
  private String out() {
    return out.toString(StandardCharsets.UTF_8);
  }

  @Nonnull
  private String err() {
    return err.toString(StandardCharsets.UTF_8);
  }

  @Nonnull
  private static String legacyParquet(
      @Nonnull final String resourceType,
      @Nonnull final String resource,
      @Nonnull final Path path) {
    return LegacyTables.writeParquet(
        LegacyTables.encode(LIBRARY_DEFAULTS, resourceType, LegacyTables.lines(resource)), path);
  }
}

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

import static au.csiro.pathling.legacy.LegacyTables.FHIR;
import static au.csiro.pathling.legacy.LegacyTables.LIBRARY_DEFAULTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemComponent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests the export component: legacy Parquet and Delta tables to FHIR NDJSON (FR-064). */
class LegacyExportTest {

  @Nonnull private static final String PATIENT = "Patient";

  @Nonnull private static final String PATIENTS = "/data/synthea/R4/Patient.ndjson";

  @TempDir Path temp;

  private LegacyExport export;

  @BeforeEach
  void setUp() {
    export = LegacyExport.of(LegacyTables.spark());
  }

  @Test
  void exportsAParquetTableToOneDocumentPerRow() {
    final List<String> source = LegacyTables.lines(PATIENTS);
    final String table =
        LegacyTables.writeParquet(
            LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, source),
            temp.resolve("Patient.parquet"));
    final Path output = temp.resolve("out");

    final List<String> written =
        export.export(PATIENT, table, LegacyTableFormat.PARQUET, output.toString());

    assertFalse(written.isEmpty(), "the export names the files it wrote");
    assertTrue(
        written.stream().map(Path::of).allMatch(file -> LegacyTables.isExportOf(file, PATIENT)),
        "every file is named for its resource type, as the NDJSON source expects: " + written);
    assertEquals(ids(source), ids(LegacyTables.exported(output, PATIENT)));
  }

  @Test
  void exportsADeltaTableToOneDocumentPerRow() {
    final List<String> source = LegacyTables.lines(PATIENTS);
    final String table =
        LegacyTables.writeDelta(
            LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, source),
            temp.resolve("Patient.parquet"));
    final Path output = temp.resolve("out");

    export.export(PATIENT, table, LegacyTableFormat.DELTA, output.toString());

    assertEquals(ids(source), ids(LegacyTables.exported(output, PATIENT)));
  }

  @Test
  void convertsATableAlreadyReadToOneDocumentPerLine() {
    final List<String> source = LegacyTables.lines(PATIENTS);
    final Dataset<Row> table = LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, source);

    final List<String> documents = export.toNdjson(PATIENT, table).collectAsList();

    assertEquals(source.size(), documents.size());
    assertTrue(
        documents.stream().noneMatch(document -> document.contains("\n")),
        "each document is one line");
    assertEquals(ids(source), ids(documents));
  }

  @Test
  void readsATableWrittenWithExtensionsDisabledAndNoNesting() {
    final FhirEncoders bare = FhirEncoders.forR4().getOrCreate();
    final List<String> source = LegacyTables.lines(PATIENTS);
    final Dataset<Row> table = LegacyTables.encode(bare, PATIENT, source);

    final List<String> documents = export.toNdjson(PATIENT, table).collectAsList();

    assertEquals(ids(source), ids(documents));
    assertTrue(
        documents.stream().noneMatch(document -> document.contains("\"extension\"")),
        "a table written without extensions has none to export");
  }

  @Test
  void readsEveryLevelOfNestingTheTableCarries() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("deep");
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    QuestionnaireItemComponent item = questionnaire.addItem().setLinkId("1");
    for (int level = 2; level <= 5; level++) {
      item = item.addItem().setLinkId(String.valueOf(level));
    }
    final FhirEncoders deep =
        FhirEncoders.forR4().withMaxNestingLevel(5).withExtensionsEnabled(true).getOrCreate();
    final Dataset<Row> table =
        LegacyTables.encode(
            deep,
            "Questionnaire",
            List.of(FHIR.newJsonParser().encodeResourceToString(questionnaire)));

    final String document = export.toNdjson("Questionnaire", table).first();

    final Questionnaire exported = (Questionnaire) FHIR.newJsonParser().parseResource(document);
    QuestionnaireItemComponent deepest = exported.getItemFirstRep();
    for (int level = 2; level <= 5; level++) {
      deepest = deepest.getItemFirstRep();
    }
    assertEquals("5", deepest.getLinkId(), "the deepest item the table carries is exported");
  }

  @Test
  void keepsTheScaleOfADecimalUpToSixPlaces() {
    final Dataset<Row> table =
        LegacyTables.encode(
            LIBRARY_DEFAULTS,
            "Observation",
            List.of(
                "{\"resourceType\":\"Observation\",\"id\":\"o\",\"status\":\"final\","
                    + "\"code\":{\"text\":\"w\"},\"valueQuantity\":{\"value\":1.50}}"));

    final String document = export.toNdjson("Observation", table).first();

    assertTrue(document.contains("\"value\":1.50"), "the stored scale is exported: " + document);
  }

  @Test
  void refusesATableInTheParquetOnFhirLayout() {
    final Dataset<Row> pof =
        LegacyTables.spark()
            .read()
            .json(
                LegacyTables.spark()
                    .createDataset(LegacyTables.lines(PATIENTS), Encoders.STRING()));

    final LegacyExportException error =
        assertThrows(LegacyExportException.class, () -> export.toNdjson(PATIENT, pof));

    assertTrue(
        error.getMessage().contains("not in the legacy layout"), "message: " + error.getMessage());
    assertTrue(
        error.getMessage().contains("Parquet on FHIR layout"), "message: " + error.getMessage());
  }

  @Test
  void refusesATableMixingTheLayouts() {
    final Dataset<Row> mixed =
        LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, LegacyTables.lines(PATIENTS))
            .withColumn("resourceType", functions.lit(PATIENT));

    final LegacyExportException error =
        assertThrows(LegacyExportException.class, () -> export.toNdjson(PATIENT, mixed));

    assertTrue(error.getMessage().contains("mixes"), "message: " + error.getMessage());
  }

  @Test
  void refusesATableThatHoldsNoResources() {
    final Dataset<Row> other =
        LegacyTables.spark().range(3).toDF("value").withColumn("name", functions.lit("x"));

    final LegacyExportException error =
        assertThrows(LegacyExportException.class, () -> export.toNdjson(PATIENT, other));

    assertTrue(
        error.getMessage().contains("not in the legacy layout"), "message: " + error.getMessage());
  }

  @Test
  void refusesAColumnTheLegacyEncoderWouldNotRead() {
    final Dataset<Row> extended =
        LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, LegacyTables.lines(PATIENTS))
            .withColumn("addedLater", functions.lit("x"));

    final LegacyExportException error =
        assertThrows(LegacyExportException.class, () -> export.toNdjson(PATIENT, extended));

    assertTrue(error.getMessage().contains("addedLater"), "message: " + error.getMessage());
  }

  @Test
  void refusesATableOfAnotherResourceType() {
    final Dataset<Row> patients =
        LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, LegacyTables.lines(PATIENTS));

    final LegacyExportException error =
        assertThrows(LegacyExportException.class, () -> export.toNdjson("Observation", patients));

    assertTrue(error.getMessage().contains("Observation"), "message: " + error.getMessage());
  }

  @Test
  void refusesAnUnreadableSource() {
    final String missing = temp.resolve("Patient.parquet").toString();
    final String output = temp.resolve("out").toString();

    final LegacyExportException error =
        assertThrows(
            LegacyExportException.class,
            () -> export.export(PATIENT, missing, LegacyTableFormat.PARQUET, output));

    assertTrue(error.getMessage().contains(missing), "message: " + error.getMessage());
  }

  @Test
  void refusesAnOutputThatAlreadyHoldsTheResourceType() throws IOException {
    final String table =
        LegacyTables.writeParquet(
            LegacyTables.encode(LIBRARY_DEFAULTS, PATIENT, LegacyTables.lines(PATIENTS)),
            temp.resolve("Patient.parquet"));
    final Path output = temp.resolve("out");
    final List<String> first =
        export.export(PATIENT, table, LegacyTableFormat.PARQUET, output.toString());

    final LegacyExportException error =
        assertThrows(
            LegacyExportException.class,
            () -> export.export(PATIENT, table, LegacyTableFormat.PARQUET, output.toString()));

    assertTrue(error.getMessage().contains(PATIENT), "message: " + error.getMessage());
    // The local file system keeps a hidden checksum beside each file, which is left out here.
    try (final Stream<Path> files = Files.list(output)) {
      assertEquals(
          first.stream().map(Path::of).map(Path::getFileName).collect(Collectors.toSet()),
          files
              .map(Path::getFileName)
              .filter(name -> !name.toString().startsWith("."))
              .collect(Collectors.toSet()),
          "the refused export leaves the output as it was");
    }
  }

  @Nonnull
  private static Set<String> ids(@Nonnull final List<String> documents) {
    return documents.stream()
        .map(document -> FHIR.newJsonParser().parseResource(document))
        .map(resource -> resource.getIdElement().getIdPart())
        .collect(Collectors.toSet());
  }
}

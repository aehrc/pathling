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
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.io.FhirWriter;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemComponent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exports a table written by the legacy encoder, re-imports the result into the Parquet on FHIR
 * layout through {@link FhirReader}, and asserts that it holds every resource of the source, equal
 * to it except for the losses the module documents (SC-012, US9).
 *
 * <p>The comparison is made on FHIR JSON: what the re-imported table writes back through {@link
 * FhirWriter} against the source with each documented loss applied by {@link DocumentedLosses}.
 * Every loss is also exercised on its own by a fixture made to carry it, so that a loss the
 * documentation lists is known to occur, and a loss it does not list fails the comparison.
 */
class LegacyRoundTripTest {

  @Nonnull private static final DefinitionContext DEFINITIONS = FhirDefinitionContext.of(FHIR);

  @TempDir Path temp;

  @Nonnull
  static Stream<Arguments> corpus() {
    return Stream.of("Patient", "Condition", "Observation", "Questionnaire")
        .flatMap(
            resourceType ->
                Stream.of(LegacyTableFormat.PARQUET, LegacyTableFormat.DELTA)
                    .map(format -> Arguments.of(resourceType, format)));
  }

  @ParameterizedTest
  @MethodSource("corpus")
  void roundTripsTheCorpus(
      @Nonnull final String resourceType, @Nonnull final LegacyTableFormat format) {
    final List<String> source = LegacyTables.lines("/data/synthea/R4/" + resourceType + ".ndjson");
    // The expected documents are made by HAPI, as the exported ones are, so HAPI must itself lose
    // nothing of the corpus, or a loss it shares with the export would go unseen.
    source.forEach(
        document ->
            assertTrue(
                SemanticJson.equal(
                    SemanticJson.parse(document),
                    SemanticJson.parse(
                        FHIR.newJsonParser()
                            .encodeResourceToString(FHIR.newJsonParser().parseResource(document)))),
                "HAPI reads and writes the document unchanged: " + document));

    final List<String> roundTripped = roundTrip(resourceType, source, format);

    assertEqualExceptDocumentedLosses(source, roundTripped);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Patient", "Observation", "ValueSet"})
  void roundTripsEachDocumentedLoss(@Nonnull final String resourceType) {
    final List<String> source = LegacyTables.lines("/data/losses/" + resourceType + ".ndjson");
    source.forEach(
        document ->
            assertFalse(
                SemanticJson.equal(
                    SemanticJson.parse(document),
                    SemanticJson.parse(DocumentedLosses.apply(document))),
                "the fixture carries a documented loss: " + document));

    final List<String> roundTripped = roundTrip(resourceType, source, LegacyTableFormat.PARQUET);

    assertEqualExceptDocumentedLosses(source, roundTripped);
  }

  @Test
  void truncatesNestingBeyondTheEncodedLevel() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("deep");
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    QuestionnaireItemComponent item = questionnaire.addItem().setLinkId("1");
    for (int level = 2; level <= 8; level++) {
      item = item.addItem().setLinkId(String.valueOf(level));
    }
    final String document = FHIR.newJsonParser().encodeResourceToString(questionnaire);

    final String roundTripped =
        roundTrip("Questionnaire", List.of(document), LegacyTableFormat.PARQUET).getFirst();

    // The library encoded three levels of nesting by default, so an item holds items to a depth
    // of four, and the levels below that were never stored.
    assertEquals(4, itemDepth(SemanticJson.parse(roundTripped)));
  }

  @Nonnull
  private List<String> roundTrip(
      @Nonnull final String resourceType,
      @Nonnull final List<String> source,
      @Nonnull final LegacyTableFormat format) {
    final Dataset<Row> legacy = LegacyTables.encode(LIBRARY_DEFAULTS, resourceType, source);
    final Path table = temp.resolve(resourceType + ".parquet");
    if (format == LegacyTableFormat.DELTA) {
      LegacyTables.writeDelta(legacy, table);
    } else {
      LegacyTables.writeParquet(legacy, table);
    }
    final Path exported = temp.resolve("ndjson");
    LegacyExport.of(LegacyTables.spark())
        .export(resourceType, table.toString(), format, exported.toString());

    final Path reimported = temp.resolve("pof").resolve(resourceType);
    FhirReader.of(LegacyTables.spark(), DEFINITIONS)
        .json()
        .read(resourceType, exported.resolve(resourceType + ".*.ndjson").toString())
        .write()
        .parquet(reimported.toString());
    final Dataset<Row> stored = LegacyTables.spark().read().parquet(reimported.toString());
    return FhirWriter.of(DEFINITIONS).json().write(resourceType, stored).collectAsList();
  }

  private static void assertEqualExceptDocumentedLosses(
      @Nonnull final List<String> source, @Nonnull final List<String> roundTripped) {
    final Map<String, JsonNode> expected = byId(source.stream().map(DocumentedLosses::apply));
    final Map<String, JsonNode> actual = byId(roundTripped.stream());
    assertEquals(source.size(), roundTripped.size(), "every resource of the source comes back");
    assertEquals(expected.keySet(), actual.keySet(), "every resource of the source comes back");
    final List<String> differences =
        expected.entrySet().stream()
            .filter(entry -> !SemanticJson.equal(entry.getValue(), actual.get(entry.getKey())))
            .map(
                entry ->
                    "expected "
                        + entry.getValue()
                        + System.lineSeparator()
                        + "  actual "
                        + actual.get(entry.getKey()))
            .toList();
    assertTrue(
        differences.isEmpty(),
        differences.size()
            + " resources differ by more than the documented losses:"
            + System.lineSeparator()
            + String.join(System.lineSeparator(), differences.stream().limit(3).toList()));
  }

  @Nonnull
  private static Map<String, JsonNode> byId(@Nonnull final Stream<String> documents) {
    return documents
        .map(SemanticJson::parse)
        .collect(Collectors.toMap(node -> node.get("id").asText(), Function.identity()));
  }

  private static int itemDepth(@Nonnull final JsonNode node) {
    final JsonNode items = node.get("item");
    return items == null || items.isEmpty() ? 0 : 1 + itemDepth(items.get(0));
  }
}

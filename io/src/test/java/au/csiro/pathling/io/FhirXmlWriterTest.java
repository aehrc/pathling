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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that resources stored in this layout are written as the FHIR XML of what was stored, which
 * keeps the XML output the previous encoder's decoding offered (FR-043, decision 84).
 */
class FhirXmlWriterTest {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A patient with repeating elements, of which one occurs once, and no primitive metadata. */
  @Nonnull
  private static final String PATIENT_JSON =
      "{\"resourceType\":\"Patient\",\"id\":\"1\","
          + "\"identifier\":[{\"system\":\"http://example.org/mrn\",\"value\":\"123\"}],"
          + "\"active\":true,"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\",\"Q\"]},{\"text\":\"J Smith\"}],"
          + "\"gender\":\"female\",\"birthDate\":\"1980-01-01\"}";

  @Test
  void selectsAFormatByItsMediaType() {
    final FhirWriter writer = TransformFixtures.fhirWriter();

    assertSame(writer.json(), writer.format("application/fhir+json"));
    assertSame(writer.xml(), writer.format("application/fhir+xml"));
    assertThrows(IllegalArgumentException.class, () -> writer.format("text/xml"));
  }

  @Test
  void writesTheXmlOfWhatWasStored() {
    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", documents(PATIENT_JSON));

    final List<String> written =
        TransformFixtures.fhirWriter().xml().write("Patient", stored).collectAsList();

    assertEquals(1, written.size());
    assertEquals(parse(PATIENT_JSON), parse(asJson(written.get(0))));
  }

  @Test
  void storesWhatItWritesAsItWasStored() {
    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", documents(PATIENT_JSON));

    final Dataset<Row> restored =
        TransformFixtures.fhirReader()
            .xml()
            .read("Patient", TransformFixtures.fhirWriter().xml().write("Patient", stored));

    assertEquals(stored.schema(), restored.schema());
    assertEquals(stored.collectAsList(), restored.collectAsList());
  }

  /**
   * XML has no way to carry the null that keeps a repeating primitive's position, which the JSON
   * writer writes until M5 stores the metadata it aligns with (decision 71). The value is kept and
   * the null is not.
   */
  @Test
  void leavesOutThePositionalNullOfARepeatingPrimitive() {
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(
                "Patient",
                documents(
                    "{\"resourceType\":\"Patient\",\"id\":\"1\","
                        + "\"name\":[{\"given\":[null,\"B\"]}]}"));

    final JsonNode written =
        parse(asJson(TransformFixtures.fhirWriter().xml().write("Patient", stored).first()));

    assertEquals(parse("[\"B\"]"), written.at("/name/0/given"));
  }

  /**
   * A structure that holds nothing, such as one whose only content was a primitive's extension, is
   * left out, as the previous writer left it out (decisions 72 and 84).
   */
  @Test
  void leavesOutAStructureThatHoldsNothing() {
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(
                "Patient",
                documents(
                    "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"_family\":"
                        + "{\"extension\":[{\"url\":\"http://example.org/x\",\"valueString\":\"y\"}]}}],"
                        + "\"maritalStatus\":{\"_text\":{\"id\":\"t\"}}}"));

    final String written = TransformFixtures.fhirWriter().xml().write("Patient", stored).first();

    assertEquals("<Patient xmlns=\"http://hl7.org/fhir\"><id value=\"1\"/></Patient>", written);
  }

  /**
   * What is written is the resource that was stored, for every type of the specification's
   * examples: the document parses, with HAPI, to the resource HAPI parses from what the JSON writer
   * writes, narrative included (L2), with decimals compared by value. It is also what the previous
   * writer wrote, which converted the JSON writer's output with HAPI, but for a narrative's
   * whitespace, which that writer collapsed and this one keeps (decision 83), and a decimal's text,
   * which this one writes as stored (owner, 2026-10-22).
   */
  @ParameterizedTest
  @MethodSource("au.csiro.pathling.io.XmlJsonParityTest#specificationTypes")
  void writesWhatWasStored(@Nonnull final String resourceType) throws Exception {
    final IParser json = FhirContext.forR4Cached().newJsonParser();
    final IParser xml = FhirContext.forR4Cached().newXmlParser();
    xml.setStripVersionsFromReferences(false);
    final List<String> source =
        java.nio.file.Files.readAllLines(SpecExampleRoundTripTest.corpus(resourceType)).stream()
            .filter(line -> !line.isBlank())
            .toList();
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(resourceType, TransformFixtures.spark().createDataset(source, Encoders.STRING()));

    final List<String> written =
        TransformFixtures.fhirWriter().xml().write(resourceType, stored).collectAsList();
    final List<String> asJson =
        TransformFixtures.writer().write(resourceType, stored).collectAsList();

    assertEquals(asJson.size(), written.size());
    for (int i = 0; i < written.size(); i++) {
      final Resource expected = (Resource) json.parseResource(asJson.get(i));
      final Resource actual = (Resource) xml.parseResource(written.get(i));
      // Decimals are compared by value: the JSON writer emits a double, and the XML writer the
      // stored text (owner, 2026-10-22).
      assertEquals(
          Optional.empty(),
          SemanticJson.difference(
              "",
              SemanticJson.parse(json.encodeResourceToString(expected)),
              SemanticJson.parse(json.encodeResourceToString(actual))),
          "not the stored resource:\n" + asJson.get(i) + "\n" + written.get(i));
      // The previous writer, but for the narratives, whose whitespace it collapsed.
      final Resource previous = (Resource) xml.parseResource(xml.encodeResourceToString(expected));
      assertEquals(
          Optional.empty(),
          SemanticJson.difference(
              "",
              withoutNarratives(SemanticJson.parse(json.encodeResourceToString(previous))),
              withoutNarratives(SemanticJson.parse(json.encodeResourceToString(actual)))),
          "not what the previous writer wrote");
    }
  }

  /** Returns a resource as JSON with every narrative left out. */
  @Nonnull
  private static JsonNode withoutNarratives(@Nonnull final JsonNode node) {
    if (node instanceof final com.fasterxml.jackson.databind.node.ObjectNode object) {
      if (object.path("div").isTextual()) {
        object.remove("div");
      }
      object.forEach(FhirXmlWriterTest::withoutNarratives);
    } else if (node.isArray()) {
      node.forEach(FhirXmlWriterTest::withoutNarratives);
    }
    return node;
  }

  /**
   * The line-delimited sink writes one document per line, a line feed in a narrative or in a value
   * written as a reference, and the XML source reads back what was stored. A carriage return, which
   * only a reference can carry in XML, is written back as one.
   */
  @Test
  void readsBackWhatTheLineDelimitedSinkWrote(@TempDir @Nonnull final Path directory) {
    final String document =
        """
        <Patient xmlns="http://hl7.org/fhir"><id value="2"/><text>\
        <status value="generated"/><div xmlns="http://www.w3.org/1999/xhtml">\
        Generated by <b>x</b>.\n  Second line&#xd;\n &amp; more<br/>\n</div></text>\
        <name><text value="line one&#xa;line two"/></name></Patient>\
        """;
    final Dataset<Row> stored =
        TransformFixtures.fhirReader()
            .xml()
            .read("Patient", documents(document))
            .unionByName(TransformFixtures.reader().read("Patient", documents(PATIENT_JSON)), true);
    final String path = directory.resolve("Patient").toString();

    TransformFixtures.fhirWriter().xml().write("Patient", stored, path, SaveMode.ErrorIfExists);

    final List<String> lines =
        TransformFixtures.spark().read().textFile(path).collectAsList().stream()
            .filter(line -> !line.isEmpty())
            .toList();
    assertEquals(2, lines.size(), "a document spans more than one line");
    assertTrue(
        lines.stream().anyMatch(line -> line.contains("x</b>.&#xa;  Second line&#xd;&#xa;")));
    final Dataset<Row> restored = TransformFixtures.fhirReader().xml().read("Patient", path);
    assertEquals(
        sorted(stored),
        sorted(
            restored.select(
                stored.columns()[0],
                java.util.Arrays.copyOfRange(stored.columns(), 1, stored.columns().length))));
  }

  /**
   * A document with no narrative and no empty element is written as Spark's XML writer wrote it, so
   * the sink keeps it to one line only because that writer writes a line break in an attribute as a
   * reference. This holds only for the writer Spark finds on the classpath, Hadoop's shaded copy of
   * Woodstox, so a change to which writer that is fails here.
   */
  @Test
  void keepsALineBreakInAnAttributeToOneLineUnfinished(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(
                "Patient",
                documents(
                    "{\"resourceType\":\"Patient\",\"id\":\"1\","
                        + "\"name\":[{\"text\":\"line one\\nline two\\r\\nline three\"}]}"));
    final String unfinished = TransformFixtures.fhirWriter().xml().write("Patient", stored).first();
    assertTrue(
        !XmlDocuments.EMPTY_ELEMENT.matcher(unfinished).find() && !unfinished.contains("<div>"),
        "the document is finished, not written as Spark's XML writer wrote it: " + unfinished);
    final String path = directory.resolve("Patient").toString();

    TransformFixtures.fhirWriter().xml().write("Patient", stored, path, SaveMode.ErrorIfExists);

    final List<String> lines =
        TransformFixtures.spark().read().textFile(path).collectAsList().stream()
            .filter(line -> !line.isEmpty())
            .toList();
    assertEquals(1, lines.size(), "a document spans more than one line");
    assertTrue(lines.get(0).contains("line one&#xa;line two&#xd;&#xa;line three"), lines.get(0));
    assertEquals(
        stored.collectAsList(),
        TransformFixtures.fhirReader().xml().read("Patient", path).collectAsList());
  }

  /**
   * A line break inside a narrative's comment cannot be written as a reference, so the sink fails
   * naming the resource rather than writing a document over two lines.
   */
  @Test
  void failsOnANarrativeLineBreakTheSinkCannotEscape(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> stored =
        TransformFixtures.reader()
            .read(
                "Patient",
                documents(
                    "{\"resourceType\":\"Patient\",\"id\":\"c1\",\"text\":{\"status\":\"generated\",\"div\":\"<div"
                        + " xmlns=\\\"http://www.w3.org/1999/xhtml\\\">a<!-- one\\n"
                        + "two --></div>\"}}"));

    final Exception failure =
        assertThrows(
            Exception.class,
            () ->
                TransformFixtures.fhirWriter()
                    .xml()
                    .write(
                        "Patient",
                        stored,
                        directory.resolve("Patient").toString(),
                        SaveMode.ErrorIfExists));

    assertTrue(
        XmlNarrativeTest.messages(failure).contains("Patient with id \"c1\""),
        XmlNarrativeTest.messages(failure));
  }

  @Nonnull
  private static List<Row> sorted(@Nonnull final Dataset<Row> dataset) {
    return dataset.collectAsList().stream()
        .sorted(java.util.Comparator.comparing(row -> row.<String>getAs("id")))
        .toList();
  }

  @Nonnull
  private static String asJson(@Nonnull final String xml) {
    final FhirContext context = FhirContext.forR4Cached();
    return context
        .newJsonParser()
        .encodeResourceToString(context.newXmlParser().parseResource(xml));
  }

  @Nonnull
  private static JsonNode parse(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json);
    } catch (final JsonProcessingException e) {
      throw new IllegalStateException("Not JSON: " + json, e);
    }
  }

  @Nonnull
  private static Dataset<String> documents(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }
}

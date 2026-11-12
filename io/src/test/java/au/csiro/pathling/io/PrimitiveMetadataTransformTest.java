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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.SchemaConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the storage of a primitive's id and extensions in the metadata group beside it, which an
 * ingest option asks for, and their egress (FR-003, ticket 17, T051).
 *
 * <p>With the option on, the group is stored and written back out in JSON and XML, for singular and
 * repeating primitives, the values and the groups of a repeating primitive aligned by position.
 * With it off, the stored schema carries no group at all (FR-016's exception 3).
 */
class PrimitiveMetadataTransformTest {

  /** The data-absent-reason extension, the usual reason a primitive carries no value. */
  @Nonnull
  private static final String DATA_ABSENT =
      "{\"extension\":[{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\","
          + "\"valueCode\":\"masked\"}]}";

  /** The same extension in FHIR XML. */
  @Nonnull
  private static final String DATA_ABSENT_XML =
      "<extension url=\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\">"
          + "<valueCode value=\"masked\"/></extension>";

  @Nonnull private static final String XMLNS = " xmlns=\"http://hl7.org/fhir\"";

  @Test
  void storesTheIdAndExtensionsOfASingularPrimitiveBesideIt(
      @TempDir @Nonnull final Path directory) {
    final String source =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"birthDate\":\"2000-01-01\","
            + "\"_birthDate\":{\"id\":\"b\",\"extension\":[{\"url\":\"http://example.org/x\","
            + "\"valueString\":\"y\"}]}}";

    final Dataset<Row> stored = storing("Patient", directory, source);
    final Row group =
        stored.selectExpr("_birthDate.id", "_birthDate.extension[0].url", "birthDate").first();

    assertEquals("b", group.getString(0));
    assertEquals("http://example.org/x", group.getString(1));
    assertEquals(
        List.of("id", "extension"),
        List.of(((StructType) stored.schema().apply("_birthDate").dataType()).fieldNames()));
    assertJsonEquals(source, json("Patient", stored));
  }

  @Test
  void alignsTheGroupsOfARepeatingPrimitiveWithItsValuesByPosition(
      @TempDir @Nonnull final Path directory) {
    final String source =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],"
            + "\"_given\":[null,"
            + DATA_ABSENT
            + ",{\"id\":\"g3\"}]}]}";

    final Dataset<Row> stored = storing("Patient", directory, source);
    final Row row =
        stored
            .selectExpr(
                "cast(name[0].given as string)",
                "size(name[0]._given)",
                "name[0]._given[0] is null",
                "name[0]._given[1].extension[0].valueCode",
                "name[0]._given[2].id")
            .first();

    assertEquals("[Ann, null, Bee]", row.getString(0));
    assertEquals(3, row.getInt(1));
    assertTrue(row.getBoolean(2), "a position without metadata keeps its null");
    assertEquals("masked", row.getString(3));
    assertEquals("g3", row.getString(4));
    assertJsonEquals(source, json("Patient", stored));
  }

  @Test
  void storesARepeatingPrimitiveCarriedOnlyAsItsGroupsAsAValueArrayOfNulls(
      @TempDir @Nonnull final Path directory) {
    // The value array is of positional nulls, one for each group, not a null array (decision 72).
    // The second resource carries the value array, so the column is observed, and the first still
    // takes the nulls row by row.
    final String metadataOnly =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"_given\":["
            + DATA_ABSENT
            + ","
            + DATA_ABSENT
            + "]}]}";
    final String withValues =
        "{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"given\":[\"Ann\"]}]}";

    final Dataset<Row> alone = storing("Patient", directory.resolve("alone"), metadataOnly);
    final Dataset<Row> mixed =
        storing("Patient", directory.resolve("mixed"), metadataOnly, withValues);

    for (final Dataset<Row> stored : List.of(alone, mixed)) {
      final Row row =
          stored
              .where("id = '1'")
              .selectExpr("size(name[0].given)", "exists(name[0].given, v -> v is not null)")
              .first();
      assertEquals(2, row.getInt(0), "the value array has one null for each group");
      assertFalse(row.getBoolean(1), "every value is null");
    }
    assertJsonEquals(metadataOnly, json("Patient", alone.where("id = '1'")));
    assertJsonEquals(metadataOnly, json("Patient", mixed.where("id = '1'")));
    assertJsonEquals(withValues, json("Patient", mixed.where("id = '2'")));
  }

  @Test
  void storesNoGroupWithTheOptionOff(@TempDir @Nonnull final Path directory) {
    final String source =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"birthDate\":\"2000-01-01\","
            + "\"_birthDate\":"
            + DATA_ABSENT
            + ",\"name\":[{\"given\":[\"Ann\",null],\"_given\":[null,"
            + DATA_ABSENT
            + "]}]}";
    final String stripped =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"birthDate\":\"2000-01-01\","
            + "\"name\":[{\"given\":[\"Ann\",null]}]}";

    directory.resolve("source").toFile().mkdirs();
    directory.resolve("stripped").toFile().mkdirs();
    final Dataset<Row> withMetadata =
        TransformFixtures.reader()
            .read("Patient", TransformFixtures.corpus(directory.resolve("source"), source));
    final Dataset<Row> withoutMetadata =
        TransformFixtures.reader()
            .read("Patient", TransformFixtures.corpus(directory.resolve("stripped"), stripped));

    assertEquals(
        withoutMetadata.schema(),
        withMetadata.schema(),
        "the option off stores the schema the source without its metadata stores");
    assertTrue(groups(withMetadata.schema()).isEmpty());
    assertJsonEquals(stripped, json("Patient", withMetadata));
  }

  /**
   * A group's content is checked against its own shape, without a finding (FR-063): what is not an
   * id or an extension is not stored, and an id is stored as text whatever the source typed it as.
   */
  @Test
  void storesOnlyTheIdAndExtensionsOfAGroup(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> bogus =
        storing(
            "Patient",
            directory.resolve("bogus"),
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"_birthDate\":{\"bogus\":1}}");
    final Dataset<Row> numeric =
        storing(
            "Patient",
            directory.resolve("numeric"),
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"_birthDate\":{\"id\":5,\"bogus\":1}}");

    assertTrue(groups(bogus.schema()).isEmpty(), "a group holding neither is not stored");
    assertTrue(
        bogus.selectExpr("birthDate is null").first().getBoolean(0),
        "the primitive is stored as a typed null all the same");
    final StructType group = (StructType) numeric.schema().apply("_birthDate").dataType();
    assertEquals(List.of("id"), List.of(group.fieldNames()));
    assertEquals("string", group.apply("id").dataType().typeName());
    assertEquals("5", numeric.selectExpr("_birthDate.id").first().getString(0));
  }

  /**
   * The extensions of a group on a resource that has no {@code extension} of its own take the
   * Extension type's shape (T061a).
   */
  @Test
  void storesTheGroupOfAPrimitiveOfAResourceWithoutExtensions(
      @TempDir @Nonnull final Path directory) {
    final String source =
        "{\"resourceType\":\"Binary\",\"id\":\"1\",\"contentType\":\"text/plain\","
            + "\"_contentType\":"
            + DATA_ABSENT
            + "}";

    final Dataset<Row> stored = storing("Binary", directory, source);

    assertEquals(
        "masked", stored.selectExpr("_contentType.extension[0].valueCode").first().getString(0));
    assertJsonEquals(source, json("Binary", stored));
  }

  @Test
  void writesOnlyTheIdAndExtensionsOfAnAnnotatedGroup(@TempDir @Nonnull final Path directory) {
    final String withId =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"birthDate\":\"2000-01-01\","
            + "\"_birthDate\":{\"id\":\"b\"}}";
    final String withoutId =
        "{\"resourceType\":\"Patient\",\"id\":\"2\",\"birthDate\":\"2001-01-01\"}";
    final Annotator annotator =
        Annotator.of(TransformFixtures.DEFINITIONS, SchemaConfiguration.builder().build());

    directory.resolve("plain").toFile().mkdirs();
    final Path stored = directory.resolve("stored");
    final Dataset<Row> annotated =
        annotator.annotate("Patient", storing("Patient", stored, withId, withoutId));
    final Dataset<Row> annotationsOnly =
        annotator.annotate(
            "Patient",
            TransformFixtures.reader()
                .read("Patient", TransformFixtures.corpus(directory.resolve("plain"), withoutId)));

    assertTrue(
        List.of(((StructType) annotated.schema().apply("_birthDate").dataType()).fieldNames())
            .contains("__start"),
        "the group carries its annotations after its id");
    assertJsonEquals(withId, json("Patient", annotated.where("id = '1'")));
    assertJsonEquals(withoutId, json("Patient", annotated.where("id = '2'")));
    assertJsonEquals(withoutId, json("Patient", annotationsOnly));
  }

  @Test
  void readsAndWritesTheGroupsOfPrimitivesInXml() {
    final String xml =
        "<Patient"
            + XMLNS
            + "><id value=\"1\"/><name><given value=\"Ann\"/><given>"
            + DATA_ABSENT_XML
            + "</given><given id=\"g3\" value=\"Bee\"/></name>"
            + "<birthDate id=\"b\" value=\"2000-01-01\">"
            + DATA_ABSENT_XML
            + "</birthDate></Patient>";
    final String expected =
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],"
            + "\"_given\":[null,"
            + DATA_ABSENT
            + ",{\"id\":\"g3\"}]}],\"birthDate\":\"2000-01-01\",\"_birthDate\":{\"id\":\"b\","
            + DATA_ABSENT.substring(1)
            + "}";

    final Dataset<Row> stored =
        TransformFixtures.metadataReader().xml().read("Patient", dataset(xml));

    assertJsonEquals(expected, json("Patient", stored));
    final String written =
        TransformFixtures.fhirWriter().xml().write("Patient", stored).collectAsList().get(0);
    assertJsonEquals(
        expected,
        json(
            "Patient", TransformFixtures.metadataReader().xml().read("Patient", dataset(written))));
    assertTrue(written.contains("<given>" + DATA_ABSENT_XML + "</given>"), written);
    assertTrue(written.contains("<given id=\"g3\" value=\"Bee\"/>"), written);
  }

  /**
   * A singular primitive carried only as its group is stored as a typed null beside the group, and
   * round-trips as the group alone, from JSON and from XML (decision 72).
   */
  @Test
  void roundTripsASingularPrimitiveCarriedOnlyAsItsGroup(@TempDir @Nonnull final Path directory) {
    final String json = "{\"resourceType\":\"Patient\",\"id\":\"1\",\"_birthDate\":{\"id\":\"b\"}}";
    final String xml = "<Patient" + XMLNS + "><id value=\"1\"/><birthDate id=\"b\"/></Patient>";

    final Dataset<Row> fromJson = storing("Patient", directory, json);
    final Dataset<Row> fromXml =
        TransformFixtures.metadataReader().xml().read("Patient", dataset(xml));

    for (final Dataset<Row> stored : List.of(fromJson, fromXml)) {
      final Row row = stored.selectExpr("birthDate is null", "_birthDate.id").first();
      assertTrue(row.getBoolean(0), "the value is a typed null");
      assertEquals("b", row.getString(1));
      assertJsonEquals(json, json("Patient", stored));
      assertEquals(
          xml,
          TransformFixtures.fhirWriter().xml().write("Patient", stored).collectAsList().get(0));
    }
  }

  /** With the option off, XML ingest leaves a primitive's id and extensions out, as JSON does. */
  @Test
  void storesNoGroupFromXmlWithTheOptionOff() {
    final String xml =
        "<Patient"
            + XMLNS
            + "><id value=\"1\"/><name><given value=\"Ann\"/><given>"
            + DATA_ABSENT_XML
            + "</given></name><birthDate id=\"b\" value=\"2000-01-01\">"
            + DATA_ABSENT_XML
            + "</birthDate></Patient>";

    final Dataset<Row> stored = TransformFixtures.fhirReader().xml().read("Patient", dataset(xml));

    assertTrue(groups(stored.schema()).isEmpty(), "no group is stored with the option off");
    assertJsonEquals(
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"given\":[\"Ann\",null]}],"
            + "\"birthDate\":\"2000-01-01\"}",
        json("Patient", stored));
  }

  @Test
  void readsARepeatingPrimitiveCarriedOnlyAsItsGroupsFromXml() {
    final String xml =
        "<Patient"
            + XMLNS
            + "><id value=\"1\"/><name><given id=\"a\"/><given>"
            + DATA_ABSENT_XML
            + "</given></name></Patient>";

    final Dataset<Row> stored =
        TransformFixtures.metadataReader().xml().read("Patient", dataset(xml));

    assertEquals(2, stored.selectExpr("size(name[0].given)").first().getInt(0));
    assertJsonEquals(
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"_given\":[{\"id\":\"a\"},"
            + DATA_ABSENT
            + "]}]}",
        json("Patient", stored));
    assertEquals(
        xml, TransformFixtures.fhirWriter().xml().write("Patient", stored).collectAsList().get(0));
  }

  /**
   * A {@code urn:} reference inside a primitive's extension resolves to the entry it names, found
   * by its type rather than by its key (decision 83), in a JSON bundle and in an XML one. The
   * {@code reference} of an {@code Expression} beside it is a {@code uri}, and keeps the same text.
   */
  @Test
  void resolvesAReferenceInsideAPrimitivesExtensionInAJsonBundle() {
    final String bundle =
        "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":["
            + "{\"fullUrl\":\"urn:uuid:o\",\"resource\":{\"resourceType\":\"Organization\","
            + "\"id\":\"o1\"}},"
            + "{\"fullUrl\":\"urn:uuid:p\",\"resource\":{\"resourceType\":\"Patient\",\"id\":\"p1\","
            + "\"gender\":\"female\",\"_gender\":{\"extension\":[{\"url\":\"http://example.org/r\","
            + "\"valueReference\":{\"reference\":\"urn:uuid:o\"}},{\"url\":\"http://example.org/e\","
            + "\"valueExpression\":{\"language\":\"text/fhirpath\",\"reference\":\"urn:uuid:o\"}}]}}}]}";

    final Dataset<Row> stored =
        TransformFixtures.metadataReader().json().readBundles("Patient", dataset(bundle));

    assertResolvedByType(stored);
  }

  @Test
  void resolvesAReferenceInsideAPrimitivesExtensionInAnXmlBundle() {
    final String bundle =
        "<Bundle"
            + XMLNS
            + "><type value=\"collection\"/>"
            + "<entry><fullUrl value=\"urn:uuid:o\"/><resource><Organization><id value=\"o1\"/>"
            + "</Organization></resource></entry>"
            + "<entry><fullUrl value=\"urn:uuid:p\"/><resource><Patient><id value=\"p1\"/>"
            + "<gender value=\"female\"><extension url=\"http://example.org/r\"><valueReference>"
            + "<reference value=\"urn:uuid:o\"/></valueReference></extension>"
            + "<extension url=\"http://example.org/e\"><valueExpression>"
            + "<language value=\"text/fhirpath\"/><reference value=\"urn:uuid:o\"/>"
            + "</valueExpression></extension></gender>"
            + "</Patient></resource></entry></Bundle>";

    final Dataset<Row> stored =
        TransformFixtures.metadataReader().xml().readBundles("Patient", dataset(bundle));

    assertResolvedByType(stored);
  }

  private static void assertResolvedByType(@Nonnull final Dataset<Row> stored) {
    final Row row =
        stored
            .selectExpr(
                "_gender.extension[0].valueReference.reference",
                "_gender.extension[1].valueExpression.reference")
            .first();
    assertEquals("Organization/o1", row.getString(0), "the Reference is resolved");
    assertEquals("urn:uuid:o", row.getString(1), "the uri named reference is kept as written");
  }

  /** Reads documents with the option on. */
  @Nonnull
  private static Dataset<Row> storing(
      @Nonnull final String resourceType,
      @Nonnull final Path directory,
      @Nonnull final String... documents) {
    directory.toFile().mkdirs();
    return TransformFixtures.metadataReader()
        .json()
        .read(resourceType, TransformFixtures.corpus(directory, documents));
  }

  /** Returns the one JSON document written from a dataset holding one resource. */
  @Nonnull
  private static String json(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    final List<String> documents =
        TransformFixtures.writer().write(resourceType, stored).collectAsList();
    assertEquals(1, documents.size());
    return documents.get(0);
  }

  private static void assertJsonEquals(
      @Nonnull final String expected, @Nonnull final String actual) {
    final JsonNode expectedNode = SemanticJson.parse(expected);
    final JsonNode actualNode = SemanticJson.parse(actual);
    final Optional<String> difference = SemanticJson.difference("", expectedNode, actualNode);
    assertTrue(difference.isEmpty(), () -> difference.orElse("") + "\n" + actual);
  }

  /** Returns the names of the metadata groups anywhere in a schema. */
  @Nonnull
  private static List<String> groups(@Nonnull final StructType schema) {
    return Stream.of(schema.fields())
        .flatMap(
            field ->
                Stream.concat(
                    field.name().startsWith("_") && !field.name().startsWith("__")
                        ? Stream.of(field.name())
                        : Stream.empty(),
                    nested(field).stream().flatMap(inner -> groups(inner).stream())))
        .toList();
  }

  @Nonnull
  private static Optional<StructType> nested(@Nonnull final StructField field) {
    DataType type = field.dataType();
    while (type instanceof final ArrayType array) {
      type = array.elementType();
    }
    return type instanceof final StructType structure ? Optional.of(structure) : Optional.empty();
  }

  @Nonnull
  private static Dataset<String> dataset(@Nonnull final String... documents) {
    return TransformFixtures.spark().createDataset(List.of(documents), Encoders.STRING());
  }
}

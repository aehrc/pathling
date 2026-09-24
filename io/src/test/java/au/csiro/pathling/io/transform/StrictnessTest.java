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

package au.csiro.pathling.io.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests the detection of content this layout does not store (FR-006).
 *
 * <p>Ignoring is the only mode, and ignoring is not silence: every case here is detected and then
 * ignored, with nothing that raises (decision 68). Detection compares the keys the source carries
 * against the fields the layout has, because the JSON reader skips a key it was not asked for in
 * every mode, so it cannot be asked to enforce this.
 */
class StrictnessTest {

  @Nonnull
  private static final String CONTAINED =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"contained\":"
          + "[{\"resourceType\":\"Observation\",\"id\":\"o\",\"status\":\"final\"}]}";

  @Nonnull
  private static final String UNDESCRIBED =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"bogusField\":\"x\","
          + "\"name\":[{\"family\":\"Smith\",\"bogusChild\":\"y\"}]}";

  @Nonnull
  private static final String ALIASED_CHOICE =
      "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
          + "\"valuePatient\":{\"reference\":\"Patient/1\"}}";

  @Nonnull
  private static final String SINGLE_WHERE_REPEATING =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":{\"family\":\"Smith\"}}";

  @Nonnull
  private static final String ARRAY_WHERE_SINGULAR =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":[\"male\"]}";

  @Nonnull
  private static final String VALUE_WHERE_STRUCTURE =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"maritalStatus\":\"married\"}";

  @Nonnull
  private static final String PRIMITIVE_METADATA =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"birthDate\":\"1980-01-01\","
          + "\"_birthDate\":{\"id\":\"a\",\"extension\":"
          + "[{\"url\":\"http://example.org/qualifier\",\"valueString\":\"approximate\"}]}}";

  @Nonnull
  private static final String PRIMITIVE_METADATA_ALONE =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"_birthDate\":{\"extension\":"
          + "[{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\","
          + "\"valueCode\":\"unknown\"}]}}";

  // Contained resources (T056).

  @Test
  void detectsContainedResources(@TempDir @Nonnull final Path directory) {
    final List<NonConformantContent> findings = findings(directory, "Patient", CONTAINED);

    assertEquals(
        List.of("Patient.contained"), paths(findings, NonConformantContent::isContainedResource));
  }

  @Test
  void ignoresContainedResources(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", CONTAINED);

    assertFalse(
        Stream.of(transformed.schema().fieldNames()).anyMatch("contained"::equals),
        "contained resources are not represented");
    assertEquals("1", transformed.first().<String>getAs("id"));
  }

  // Content the definitions do not describe (T057).

  @Test
  void detectsContentTheDefinitionsDoNotDescribe(@TempDir @Nonnull final Path directory) {
    final List<NonConformantContent> findings = findings(directory, "Patient", UNDESCRIBED);

    assertEquals(
        List.of("Patient.bogusField", "Patient.name.bogusChild"),
        paths(findings, NonConformantContent::isUndescribedContent).stream().sorted().toList());
  }

  @Test
  void detectsAChoiceVariantTheLayoutCarriesNoColumnFor(@TempDir @Nonnull final Path directory) {
    // The definition library resolves `valuePatient` as an alias of the reference variant, but the
    // layout carries one `valueReference` column rather than one per target resource type, so the
    // content has nowhere to go and must be reported rather than dropped.
    final List<NonConformantContent> findings = findings(directory, "Observation", ALIASED_CHOICE);

    assertEquals(
        List.of("Observation.valuePatient"),
        paths(findings, NonConformantContent::isUndescribedContent));
  }

  @Test
  void ignoresContentTheDefinitionsDoNotDescribe(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", UNDESCRIBED);

    assertFalse(
        Stream.of(transformed.schema().fieldNames()).anyMatch("bogusField"::equals),
        "content outside the definitions is not stored");
    final Row first = transformed.first();
    final List<Row> names = first.getList(first.fieldIndex("name"));
    assertEquals("Smith", names.get(0).<String>getAs("family"));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "Patient|{\"resourceType\":\"Patient\",\"id\":\"1\",\"birthDate\":\"1970\","
            + "\"__birthDate_start\":\"whatever\"}|Patient.__birthDate_start",
        "Observation|{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
            + "\"__valueQuantity_canonical\":{\"value\":\"oops\"}}"
            + "|Observation.__valueQuantity_canonical"
      })
  void detectsASourceKeyNamedAsALayoutAnnotation(
      @Nonnull final String resourceType,
      @Nonnull final String document,
      @Nonnull final String path,
      @TempDir @Nonnull final Path directory) {
    // An annotation is derived by the layout, never read from the source, so a key carrying its
    // name is content the definitions do not describe. It is not checked as the element the
    // annotation accompanies, which would report nothing, or report it as that element.
    final List<NonConformantContent> findings = findings(directory, resourceType, document);

    assertEquals(List.of(path), paths(findings, finding -> true));
    assertEquals(List.of(path), paths(findings, NonConformantContent::isUndescribedContent));
  }

  // Cardinality contradicting the definitions (T057a).

  @Test
  void detectsARepeatingElementSuppliedAsASingleObject(@TempDir @Nonnull final Path directory) {
    final List<NonConformantContent> findings =
        findings(directory, "Patient", SINGLE_WHERE_REPEATING);

    assertEquals(List.of("Patient.name"), paths(findings, NonConformantContent::isShapeMismatch));
  }

  @Test
  void detectsASingularElementSuppliedAsAnArray(@TempDir @Nonnull final Path directory) {
    final List<NonConformantContent> findings =
        findings(directory, "Patient", ARRAY_WHERE_SINGULAR);

    assertEquals(List.of("Patient.gender"), paths(findings, NonConformantContent::isShapeMismatch));
  }

  @Test
  void neverCoercesARepeatingElementSuppliedAsASingleObject(
      @TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", SINGLE_WHERE_REPEATING);

    assertTrue(
        transformed.schema().apply("name").dataType() instanceof ArrayType,
        "cardinality comes from the definitions, not from the data");
    assertNull(transformed.first().getAs("name"), "the contradicted content is not coerced");
  }

  @Test
  void neverCoercesASingularElementSuppliedAsAnArray(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", ARRAY_WHERE_SINGULAR);

    assertEquals(DataTypes.StringType, transformed.schema().apply("gender").dataType());
    assertNull(transformed.first().getAs("gender"), "an array is not rendered as its own text");
  }

  @Test
  void detectsAValueSuppliedWhereAStructureIsDeclared(@TempDir @Nonnull final Path directory) {
    final List<NonConformantContent> findings =
        findings(directory, "Patient", VALUE_WHERE_STRUCTURE);

    assertEquals(
        List.of("Patient.maritalStatus"), paths(findings, NonConformantContent::isShapeMismatch));
  }

  @Test
  void dropsAValueSuppliedWhereAStructureIsDeclared(@TempDir @Nonnull final Path directory) {
    // A structure needs at least one field, and a plain value names none, so there is no type the
    // null could take and the element does not appear at all. The finding above is what reports it.
    final Dataset<Row> transformed = transform(directory, "Patient", VALUE_WHERE_STRUCTURE);

    assertFalse(
        Stream.of(transformed.schema().fieldNames()).anyMatch("maritalStatus"::equals),
        "a structure supplied as a value is not stored");
    assertEquals("1", transformed.first().<String>getAs("id"));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "Patient|{\"resourceType\":\"Patient\",\"id\":\"1\","
            + "\"name\":[{\"family\":\"x\",\"given\":[[\"a\",\"b\"]]}]}"
            + "|Patient.name.given|name[0].given is null and name[0].family = 'x'",
        "Patient|{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[[{\"family\":\"x\"}]]}"
            + "|Patient.name|name is null",
        "Claim|{\"resourceType\":\"Claim\",\"id\":\"1\","
            + "\"item\":[{\"sequence\":1,\"careTeamSequence\":[[1,2]]}]}"
            + "|Claim.item.careTeamSequence"
            + "|item[0].careTeamSequence is null and item[0].sequence = 1"
      })
  void detectsAndNeverStoresAnArrayOfArraysWhereARepeatingElementIsDeclared(
      @Nonnull final String resourceType,
      @Nonnull final String document,
      @Nonnull final String path,
      @Nonnull final String stored,
      @TempDir @Nonnull final Path directory) {
    // FHIR JSON has no arrays of arrays. Unwrapped, a text value would be stored as the text of its
    // inner array, a structure's fields would be read one level too deep, and a number would fail
    // the whole read, so the nesting is a contradiction like any other.
    final List<NonConformantContent> findings = findings(directory, resourceType, document);
    final Dataset<Row> transformed = transform(directory, resourceType, document);

    assertEquals(List.of(path), paths(findings, NonConformantContent::isShapeMismatch));
    assertTrue(
        transformed.selectExpr(stored).first().getBoolean(0),
        "the nested content is not stored, and its neighbours are");
  }

  // Primitive id and extension content (T057b).

  @Test
  void detectsPrimitiveIdAndExtensionContent(@TempDir @Nonnull final Path directory) {
    final List<NonConformantContent> findings = findings(directory, "Patient", PRIMITIVE_METADATA);

    assertEquals(
        List.of("Patient._birthDate"), paths(findings, NonConformantContent::isPrimitiveMetadata));
  }

  @Test
  void detectsPrimitiveMetadataCarriedWithoutAValue(@TempDir @Nonnull final Path directory) {
    // A data-absent-reason carries the extension with no value beside it, which is conformant FHIR
    // and must be reported as metadata rather than as content the definitions do not describe.
    final List<NonConformantContent> findings =
        findings(directory, "Patient", PRIMITIVE_METADATA_ALONE);

    assertEquals(
        List.of("Patient._birthDate"), paths(findings, NonConformantContent::isPrimitiveMetadata));
    assertEquals(List.of(), paths(findings, NonConformantContent::isUndescribedContent));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "{\"_family\":\"oops\"}|Patient.name._family",
        "{\"_family\":[{\"id\":\"f\"}]}|Patient.name._family",
        "{\"_given\":{\"id\":\"f\"}}|Patient.name._given",
        "{\"_given\":[[{\"id\":\"f\"}]]}|Patient.name._given",
        "{\"_given\":[null]}|Patient.name._given"
      })
  void detectsAMalformedMetadataGroupAsAShapeMismatch(
      @Nonnull final String name,
      @Nonnull final String path,
      @TempDir @Nonnull final Path directory) {
    // A group in a shape FHIR does not give it keeps nothing of its element (decision 72), so it
    // must not be reported as the metadata that is merely not yet stored.
    final List<NonConformantContent> findings =
        findings(
            directory,
            "Patient",
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[" + name + "]}");

    assertEquals(List.of(path), paths(findings, NonConformantContent::isShapeMismatch));
    assertEquals(List.of(), paths(findings, NonConformantContent::isPrimitiveMetadata));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "{\"_family\":{\"id\":\"f\"}}|{\"_family\":\"oops\"}|Patient.name._family",
        "{\"_given\":[{\"id\":\"g\"}]}|{\"_given\":{\"id\":\"g\"}}|Patient.name._given"
      })
  void omitsTheStructureOfAWellFormedMetadataGroupThatAnotherDocumentReTyped(
      @Nonnull final String conformantName,
      @Nonnull final String malformedName,
      @Nonnull final String path,
      @TempDir @Nonnull final Path directory) {
    // The first document is conformant on its own. The second gives the group another shape, which
    // types the column for the whole input, so the group keeps nothing in either document. Nothing
    // else keeps the name's column, so the first document's name is lost with it, and the loss is
    // reported as the contradiction that caused it (FR-016 item 6).
    final String[] documents = documents(conformantName, malformedName);

    assertEquals(
        List.of(path),
        paths(findings(directory, "Patient", documents), NonConformantContent::isShapeMismatch));
    assertFalse(
        Stream.of(transform(directory, "Patient", documents).schema().fieldNames())
            .anyMatch("name"::equals),
        "the conformant document's name is lost with the re-typed group");
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "{\"_family\":{\"id\":\"f\"}}|{\"family\":\"X\",\"_family\":\"oops\"}|Patient.name._family",
        "{\"_given\":[{\"id\":\"g\"}]}|{\"family\":\"X\",\"_given\":{\"id\":\"g\"}}|Patient.name._given"
      })
  void keepsTheStructureOfAReTypedMetadataGroupWhereSomethingElseKeepsItsColumn(
      @Nonnull final String conformantName,
      @Nonnull final String malformedName,
      @Nonnull final String path,
      @TempDir @Nonnull final Path directory) {
    // As above, but the second document's family keeps the name's column, so the first document's
    // name, emptied by the re-typed group, is kept as an empty structure rather than omitted.
    final String[] documents = documents(conformantName, malformedName);
    final Row first =
        transform(directory, "Patient", documents)
            .where("id = '1'")
            .selectExpr("size(name)", "name[0] is not null", "name[0].family is null")
            .first();

    assertEquals(
        List.of(path),
        paths(findings(directory, "Patient", documents), NonConformantContent::isShapeMismatch));
    assertEquals(1, first.getInt(0), "the conformant document's name is kept");
    assertTrue(first.getBoolean(1), "the conformant document's name is present, not null");
    assertTrue(first.getBoolean(2), "nothing of the conformant document's name is stored");
  }

  @Test
  void dropsPrimitiveMetadataFromTheFittedSchema(@TempDir @Nonnull final Path directory) {
    // The metadata group is not written until M5, so the fitted schema, which is the schema of what
    // is actually written, carries no group at all.
    final Dataset<Row> transformed = transform(directory, "Patient", PRIMITIVE_METADATA);

    assertFalse(
        Stream.of(transformed.schema().fieldNames()).anyMatch("_birthDate"::equals),
        "an unpopulated metadata group is not part of the fitted schema");
    assertEquals("1980-01-01", transformed.first().<String>getAs("birthDate"));
  }

  /** Returns a conformant document and one whose name re-types its metadata group. */
  @Nonnull
  private static String[] documents(
      @Nonnull final String conformantName, @Nonnull final String malformedName) {
    return new String[] {
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[" + conformantName + "]}",
      "{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[" + malformedName + "]}"
    };
  }

  @Nonnull
  private static List<NonConformantContent> findings(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      @Nonnull final String... documents) {
    final Dataset<Row> source =
        TransformFixtures.spark().read().json(TransformFixtures.corpus(directory, documents));
    return StrictnessCheck.of(
            DefinitionCanonicalStructure.forResource(TransformFixtures.DEFINITIONS, resourceType),
            resourceType)
        .check(source.schema());
  }

  @Nonnull
  private static Dataset<Row> transform(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      @Nonnull final String... documents) {
    return TransformFixtures.reader()
        .read(resourceType, TransformFixtures.corpus(directory, documents));
  }

  @Nonnull
  private static List<String> paths(
      @Nonnull final List<NonConformantContent> findings,
      @Nonnull final Predicate<NonConformantContent> kind) {
    return findings.stream().filter(kind).map(NonConformantContent::getPath).toList();
  }
}

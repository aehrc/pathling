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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.SchemaConfiguration;
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

/**
 * Tests the strictness switch over content the definition set does not describe (FR-006, FR-018).
 *
 * <p>Ignoring is the default, and ignoring is not silence: every case here is detected in both
 * modes, and differs only in whether the detection raises. Detection compares the keys the source
 * carries against the fields the layout has, because the JSON reader skips a key it was not asked
 * for in every mode, so it cannot be asked to enforce this.
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
  void failsOnContainedResourcesWhenStrict(@TempDir @Nonnull final Path directory) {
    final InvalidUserInputError error =
        assertThrows(
            InvalidUserInputError.class, () -> transform(directory, "Patient", true, CONTAINED));

    assertTrue(error.getMessage().contains("Patient.contained"), error.getMessage());
  }

  @Test
  void ignoresContainedResourcesByDefault(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", false, CONTAINED);

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
  void failsOnContentTheDefinitionsDoNotDescribeWhenStrict(@TempDir @Nonnull final Path directory) {
    final InvalidUserInputError error =
        assertThrows(
            InvalidUserInputError.class, () -> transform(directory, "Patient", true, UNDESCRIBED));

    assertTrue(error.getMessage().contains("Patient.bogusField"), error.getMessage());
  }

  @Test
  void ignoresContentTheDefinitionsDoNotDescribeByDefault(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", false, UNDESCRIBED);

    assertFalse(
        Stream.of(transformed.schema().fieldNames()).anyMatch("bogusField"::equals),
        "content outside the definitions is not stored");
    assertEquals("Smith", transformed.first().<List<Row>>getAs("name").get(0).getAs("family"));
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
  void failsOnCardinalityContradictingTheDefinitionsWhenStrict(
      @TempDir @Nonnull final Path directory) {
    final InvalidUserInputError error =
        assertThrows(
            InvalidUserInputError.class,
            () -> transform(directory, "Patient", true, SINGLE_WHERE_REPEATING));

    assertTrue(error.getMessage().contains("Patient.name"), error.getMessage());
  }

  @Test
  void neverCoercesARepeatingElementSuppliedAsASingleObject(
      @TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", false, SINGLE_WHERE_REPEATING);

    assertTrue(
        transformed.schema().apply("name").dataType() instanceof ArrayType,
        "cardinality comes from the definitions, not from the data");
    assertNull(transformed.first().getAs("name"), "the contradicted content is not coerced");
  }

  @Test
  void neverCoercesASingularElementSuppliedAsAnArray(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory, "Patient", false, ARRAY_WHERE_SINGULAR);

    assertEquals(DataTypes.StringType, transformed.schema().apply("gender").dataType());
    assertNull(transformed.first().getAs("gender"), "an array is not rendered as its own text");
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

  @Test
  void failsOnPrimitiveMetadataWhenStrict(@TempDir @Nonnull final Path directory) {
    final InvalidUserInputError error =
        assertThrows(
            InvalidUserInputError.class,
            () -> transform(directory, "Patient", true, PRIMITIVE_METADATA));

    assertTrue(error.getMessage().contains("Patient._birthDate"), error.getMessage());
  }

  @Test
  void dropsPrimitiveMetadataFromTheFittedSchemaByDefault(@TempDir @Nonnull final Path directory) {
    // The metadata group is derived by the schema builder but not populated until M5, so the fitted
    // schema is the schema of what is actually written and carries no group at all.
    final Dataset<Row> transformed = transform(directory, "Patient", false, PRIMITIVE_METADATA);

    assertFalse(
        Stream.of(transformed.schema().fieldNames()).anyMatch("_birthDate"::equals),
        "an unpopulated metadata group is not part of the fitted schema");
    assertEquals("1980-01-01", transformed.first().<String>getAs("birthDate"));
  }

  @Nonnull
  private static List<NonConformantContent> findings(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      @Nonnull final String... documents) {
    final Dataset<Row> source =
        TransformFixtures.spark()
            .read()
            .options(DecimalTransform.lexicalReadOptions())
            .json(TransformFixtures.corpus(directory, documents));
    return StrictnessCheck.of(
            DefinitionCanonicalStructure.forResource(TransformFixtures.DEFINITIONS, resourceType),
            resourceType)
        .check(source.schema());
  }

  @Nonnull
  private static Dataset<Row> transform(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      final boolean strict,
      @Nonnull final String... documents) {
    final SchemaConfiguration configuration =
        SchemaConfiguration.builder().failOnNonConformantContent(strict).build();
    return TransformFixtures.transformer(configuration)
        .read(
            TransformFixtures.spark(),
            resourceType,
            TransformFixtures.corpus(directory, documents));
  }

  @Nonnull
  private static List<String> paths(
      @Nonnull final List<NonConformantContent> findings,
      @Nonnull final Predicate<NonConformantContent> kind) {
    return findings.stream().filter(kind).map(NonConformantContent::getPath).toList();
  }
}

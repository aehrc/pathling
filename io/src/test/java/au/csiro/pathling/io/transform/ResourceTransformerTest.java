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
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that the transform writes the types, cardinality and field order the definitions give, and
 * nothing of the schema the source happened to be read with (FR-008, FR-012).
 *
 * <p>The inferred schema orders its fields alphabetically and types every value from the data
 * alone, so a transform that carried any of it across would be visible here.
 */
class ResourceTransformerTest {

  /** The keys are deliberately out of definition order, and one of them repeats. */
  @Nonnull
  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"gender\":\"male\",\"multipleBirthInteger\":2,"
          + "\"birthDate\":\"1980-01-01\",\"id\":\"1\",\"active\":true,"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\",\"Elizabeth\"]}]}";

  @Test
  void ordersFieldsAsTheDefinitionsDeclareThem(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory);

    assertEquals(
        List.of(
            "resourceType", "id", "active", "name", "gender", "birthDate", "multipleBirthInteger"),
        List.of(transformed.schema().fieldNames()));
  }

  @Test
  void takesTypesAndCardinalityFromTheDefinitions(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory);
    final StructType schema = transformed.schema();

    assertEquals(DataTypes.BooleanType, schema.apply("active").dataType());
    assertEquals(DataTypes.IntegerType, schema.apply("multipleBirthInteger").dataType());
    assertEquals(DataTypes.StringType, schema.apply("gender").dataType());
    assertEquals(
        DataTypes.createArrayType(DataTypes.StringType, true),
        ((StructType) ((ArrayType) schema.apply("name").dataType()).elementType())
            .apply("given")
            .dataType());

    final Row first = transformed.first();
    assertEquals(Boolean.TRUE, first.getAs("active"));
    assertEquals(2, first.<Integer>getAs("multipleBirthInteger"));
  }

  // Types and cardinality come from the definitions, never from the data (T028).

  @Test
  void storesARepeatingElementCarryingOneValueAsAnArray(@TempDir @Nonnull final Path directory) {
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\","
                    + "\"name\":[{\"given\":[\"Jane\"]}],\"identifier\":[{\"value\":\"a\"}]}")
            .schema();

    assertTrue(schema.apply("identifier").dataType() instanceof ArrayType);
    assertTrue(schema.apply("name").dataType() instanceof ArrayType);
    assertEquals(
        DataTypes.createArrayType(DataTypes.StringType, true),
        structAt(schema, "name").apply("given").dataType());
  }

  @Test
  void storesTheTypeTheDefinitionsGiveRatherThanTheOneInferred(
      @TempDir @Nonnull final Path directory) {
    // Inference types the value as a double; the definitions store a decimal as text.
    final StructType schema =
        transform(
                directory,
                "Observation",
                "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                    + "\"valueQuantity\":{\"value\":1.5}}")
            .schema();

    assertEquals(DataTypes.StringType, structAt(schema, "valueQuantity").apply("value").dataType());
  }

  // A structure survives only where some element beneath it is stored (T029).

  @Test
  void omitsAStructureWhoseOnlyContentIsUndescribed(@TempDir @Nonnull final Path directory) {
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"bogusChild\":\"y\"}]}")
            .schema();

    assertFalse(
        List.of(schema.fieldNames()).contains("name"), List.of(schema.fieldNames()).toString());
  }

  @Test
  void keepsAStructureWhoseOnlyContentIsPrimitiveMetadata(@TempDir @Nonnull final Path directory) {
    // The metadata group is not written before M5, but the primitive it belongs to is stored as a
    // null of its declared type, so the structure holding it is not empty (decision 72).
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\","
                    + "\"name\":[{\"_family\":{\"id\":\"f\"}}]}")
            .schema();

    assertEquals(
        "array<struct<family:string>>",
        schema.apply("name").dataType().simpleString(),
        schema.treeString());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"_family\":\"oops\"}",
        "{\"_family\":[{\"id\":\"f\"}]}",
        "{\"_family\":null}",
        "{\"_given\":{\"id\":\"f\"}}",
        "{\"_given\":\"oops\"}",
        "{\"_given\":[[{\"id\":\"f\"}]]}",
        "{\"_given\":[null]}"
      })
  void omitsAStructureWhoseOnlyContentIsAMalformedMetadataGroup(
      @Nonnull final String name, @TempDir @Nonnull final Path directory) {
    // A metadata group in a shape FHIR does not give it is non-conformant content, and does not
    // keep the structure the way a well-formed one does (decision 72). The shape depends on whether
    // the primitive repeats, so both a singular and a repeating primitive are covered.
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[" + name + "]}")
            .schema();

    assertFalse(
        List.of(schema.fieldNames()).contains("name"), List.of(schema.fieldNames()).toString());
  }

  @Test
  void omitsEveryAncestorLeftEmptyBeneathIt(@TempDir @Nonnull final Path directory) {
    // The rule applies after the children are settled, so emptiness propagates upward.
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\","
                    + "\"contact\":[{\"name\":{\"bogusChild\":\"y\"}}]}")
            .schema();

    assertFalse(
        List.of(schema.fieldNames()).contains("contact"), List.of(schema.fieldNames()).toString());
  }

  @Test
  void keepsAStructureWhereOneElementBeneathItIsStored(@TempDir @Nonnull final Path directory) {
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\","
                    + "\"contact\":[{\"name\":{\"bogusChild\":\"y\",\"family\":\"S\"}}]}")
            .schema();

    assertEquals(List.of("family"), List.of(structAt(schema, "contact", "name").fieldNames()));
  }

  // Every structure orders its fields as the definitions declare them (T030a).

  @Test
  void ordersTheFieldsOfNestedStructuresAsTheDefinitionsDeclareThem(
      @TempDir @Nonnull final Path directory) {
    final StructType schema =
        transform(
                directory,
                "Patient",
                "{\"resourceType\":\"Patient\",\"id\":\"1\","
                    + "\"name\":[{\"given\":[\"J\"],\"use\":\"official\",\"family\":\"S\","
                    + "\"text\":\"J S\"}],"
                    + "\"contact\":[{\"name\":{\"period\":{\"end\":\"2020\",\"start\":\"2019\"},"
                    + "\"family\":\"T\"}}],"
                    + "\"extension\":[{\"valueString\":\"v\",\"url\":\"http://example.org\"}]}")
            .schema();

    assertEquals(
        List.of("use", "text", "family", "given"), List.of(structAt(schema, "name").fieldNames()));
    assertEquals(
        List.of("family", "period"), List.of(structAt(schema, "contact", "name").fieldNames()));
    assertEquals(
        List.of("start", "end"),
        List.of(structAt(schema, "contact", "name", "period").fieldNames()));
    assertEquals(
        List.of("url", "valueString"), List.of(structAt(schema, "extension").fieldNames()));
  }

  @Nonnull
  private static Dataset<Row> transform(
      @Nonnull final Path directory,
      @Nonnull final String resourceType,
      @Nonnull final String document) {
    return TransformFixtures.reader()
        .read(resourceType, TransformFixtures.corpus(directory, document));
  }

  /** Returns the structure reached by following field names, unwrapping arrays on the way. */
  @Nonnull
  private static StructType structAt(
      @Nonnull final StructType schema, @Nonnull final String... path) {
    DataType current = schema;
    for (final String name : path) {
      current = ((StructType) elementTypeOf(current)).apply(name).dataType();
    }
    return (StructType) elementTypeOf(current);
  }

  @Nonnull
  private static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? array.elementType() : type;
  }

  @Nonnull
  private static Dataset<Row> transform(@Nonnull final Path directory) {
    final String path = TransformFixtures.corpus(directory, PATIENT);
    return TransformFixtures.reader().read("Patient", path);
  }
}

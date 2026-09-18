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

import au.csiro.pathling.schema.SchemaBuilder;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that the transform writes the schema the definitions derive, and nothing of the schema the
 * source happened to be read with (FR-008, FR-012).
 *
 * <p>The inferred schema orders its fields alphabetically and types every primitive as text, so a
 * transform that carried any of it across would be visible here.
 */
class ResourceTransformerTest {

  /** The keys are deliberately out of definition order, and one of them repeats. */
  @Nonnull
  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"gender\":\"male\",\"multipleBirthInteger\":2,"
          + "\"birthDate\":\"1980-01-01\",\"id\":\"1\",\"active\":true,"
          + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Jane\",\"Elizabeth\"]}]}";

  @Test
  void producesTheSchemaTheDefinitionsDerive(@TempDir @Nonnull final Path directory) {
    final String path = TransformFixtures.corpus(directory, PATIENT);
    final StructType observed =
        TransformFixtures.spark()
            .read()
            .options(DecimalTransform.lexicalReadOptions())
            .json(path)
            .schema();
    final StructType derived =
        SchemaBuilder.of(
                TransformFixtures.DEFINITIONS, 0, false, TransformFixtures.STANDARD_OPEN_TYPES)
            .pruned("Patient", observed);

    final Dataset<Row> transformed =
        TransformFixtures.transformer().read(TransformFixtures.spark(), "Patient", path);

    assertEquals(derived, transformed.schema());
  }

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

  @Nonnull
  private static Dataset<Row> transform(@Nonnull final Path directory) {
    final String path = TransformFixtures.corpus(directory, PATIENT);
    return TransformFixtures.transformer().read(TransformFixtures.spark(), "Patient", path);
  }
}

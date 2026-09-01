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

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SchemaDrift}. The comparison must count only missing field paths as drift,
 * recursing through structs, arrays and maps, while ignoring nullability and column metadata
 * differences and tolerating extra fields that exist only in the target schema.
 *
 * @author John Grimes
 */
class SchemaDriftTest {

  // Verifies that a source field absent from the target at the top level is detected as drift.
  @Test
  void missingTopLevelFieldIsDrift() {
    final StructType source =
        struct(field("id", DataTypes.StringType), field("url", DataTypes.StringType));
    final StructType target = struct(field("id", DataTypes.StringType));

    assertThat(SchemaDrift.hasMissingFields(source, target)).isTrue();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).containsExactly("url");
  }

  // Verifies that a field missing inside a nested struct is detected as drift.
  @Test
  void missingNestedStructFieldIsDrift() {
    final StructType sourceInner =
        struct(field("name", DataTypes.StringType), field("version", DataTypes.StringType));
    final StructType targetInner = struct(field("name", DataTypes.StringType));
    final StructType source = struct(field("id", DataTypes.StringType), field("meta", sourceInner));
    final StructType target = struct(field("id", DataTypes.StringType), field("meta", targetInner));

    assertThat(SchemaDrift.hasMissingFields(source, target)).isTrue();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).containsExactly("meta.version");
  }

  // Verifies that a field missing from a struct nested inside an array element is detected as
  // drift.
  @Test
  void missingFieldInsideArrayElementStructIsDrift() {
    final StructType sourceElement =
        struct(field("path", DataTypes.StringType), field("forEach", DataTypes.StringType));
    final StructType targetElement = struct(field("path", DataTypes.StringType));
    final StructType source = struct(field("select", ArrayType.apply(sourceElement)));
    final StructType target = struct(field("select", ArrayType.apply(targetElement)));

    assertThat(SchemaDrift.hasMissingFields(source, target)).isTrue();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).containsExactly("select.forEach");
  }

  // Verifies that a field missing from a struct used as a map value type is detected as drift.
  @Test
  void missingFieldInsideMapValueStructIsDrift() {
    final StructType sourceValue =
        struct(field("code", DataTypes.StringType), field("display", DataTypes.StringType));
    final StructType targetValue = struct(field("code", DataTypes.StringType));
    final StructType source =
        struct(field("codings", MapType.apply(DataTypes.StringType, sourceValue)));
    final StructType target =
        struct(field("codings", MapType.apply(DataTypes.StringType, targetValue)));

    assertThat(SchemaDrift.hasMissingFields(source, target)).isTrue();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).containsExactly("codings.display");
  }

  // Verifies that a difference in nullability alone is not treated as drift.
  @Test
  void nullabilityOnlyDifferenceIsNotDrift() {
    final StructType source =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, false, Metadata.empty())
            });
    final StructType target =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, Metadata.empty())
            });

    assertThat(SchemaDrift.hasMissingFields(source, target)).isFalse();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).isEmpty();
  }

  // Verifies that a difference in column metadata alone is not treated as drift.
  @Test
  void metadataOnlyDifferenceIsNotDrift() {
    final Metadata metadata = new MetadataBuilder().putString("comment", "annotated").build();
    final StructType source =
        new StructType(
            new StructField[] {new StructField("id", DataTypes.StringType, true, metadata)});
    final StructType target =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, Metadata.empty())
            });

    assertThat(SchemaDrift.hasMissingFields(source, target)).isFalse();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).isEmpty();
  }

  // Verifies that fields present only in the target schema (for example, data written by a newer
  // server version) are not treated as drift.
  @Test
  void extraTargetOnlyFieldIsNotDrift() {
    final StructType source = struct(field("id", DataTypes.StringType));
    final StructType target =
        struct(field("id", DataTypes.StringType), field("newField", DataTypes.StringType));

    assertThat(SchemaDrift.hasMissingFields(source, target)).isFalse();
    assertThat(SchemaDrift.missingFieldPaths(source, target)).isEmpty();
  }

  // Verifies that identical schemas produce no drift.
  @Test
  void identicalSchemasAreNotDrift() {
    final StructType schema =
        struct(field("id", DataTypes.StringType), field("status", DataTypes.StringType));

    assertThat(SchemaDrift.hasMissingFields(schema, schema)).isFalse();
    assertThat(SchemaDrift.missingFieldPaths(schema, schema)).isEmpty();
  }

  // ---- helpers ----

  @Nonnull
  private static StructField field(@Nonnull final String name, @Nonnull final DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }

  @Nonnull
  private static StructType struct(@Nonnull final StructField... fields) {
    return new StructType(fields);
  }
}

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

package au.csiro.pathling.library.io;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Tests the schema equality of spec SC-002 (T011). */
class SchemaEqualityTest {

  @Nonnull
  private static final StructType SCHEMA =
      StructType.fromDDL(
          "id STRING, name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,"
              + " tags MAP<STRING, STRUCT<code: STRING>>");

  @Nonnull
  private static final Metadata METADATA = new MetadataBuilder().putString("comment", "x").build();

  /** The schema with every field, array element and map value non-nullable, and metadata. */
  @Nonnull
  private static StructType strict() {
    final StructType name =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("family", DataTypes.StringType, false, METADATA),
              DataTypes.createStructField(
                  "given", DataTypes.createArrayType(DataTypes.StringType, false), false)
            });
    final StructType code =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("code", DataTypes.StringType, false, METADATA)
            });
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.StringType, false, METADATA),
          DataTypes.createStructField("name", DataTypes.createArrayType(name, false), false),
          DataTypes.createStructField(
              "tags", DataTypes.createMapType(DataTypes.StringType, code, false), false)
        });
  }

  @Test
  void ignoresNullabilityAndMetadataAtEveryDepth() {
    assertTrue(SchemaEquality.same(SCHEMA, SCHEMA));
    assertTrue(SchemaEquality.same(SCHEMA, strict()));
    assertTrue(SchemaEquality.same(strict(), SCHEMA));
  }

  @Test
  void ignoresAnnotationFields() {
    final StructType annotated =
        StructType.fromDDL(
            "id STRING, __first DECIMAL(38,6),"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>, __flag: BOOLEAN>>,"
                + " tags MAP<STRING, STRUCT<code: STRING, __canonical_code: STRING>>,"
                + " __custom STRING");

    assertTrue(SchemaEquality.same(SCHEMA, annotated));
    assertTrue(SchemaEquality.same(annotated, SCHEMA));
  }

  @Test
  void dropsGroupLeftWithOnlyAnnotationFields() {
    final StructType grouped =
        StructType.fromDDL(
            "id STRING, _id STRUCT<__start: TIMESTAMP, __end: TIMESTAMP>,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>,"
                + " _given: ARRAY<STRUCT<__flag: BOOLEAN>>>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");
    final StructType groupWithBaseField =
        StructType.fromDDL(
            "id STRING, _id STRUCT<id: STRING, __start: TIMESTAMP>,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");

    assertTrue(SchemaEquality.same(SCHEMA, grouped));
    assertTrue(SchemaEquality.same(grouped, SCHEMA));
    assertFalse(SchemaEquality.same(SCHEMA, groupWithBaseField));
    assertFalse(SchemaEquality.same(groupWithBaseField, SCHEMA));
  }

  @Test
  void dropsGroupLeftWithOnlyAnnotationFieldsAtAnyDepth() {
    final StructType nested =
        StructType.fromDDL(
            "id STRING, _id STRUCT<_inner: ARRAY<STRUCT<__flag: BOOLEAN>>, __start: TIMESTAMP>,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");
    final StructType nestedWithBaseField =
        StructType.fromDDL(
            "id STRING, _id STRUCT<_inner: STRUCT<id: STRING, __flag: BOOLEAN>>,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");

    assertTrue(SchemaEquality.same(SCHEMA, nested));
    assertTrue(SchemaEquality.same(nested, SCHEMA));
    assertFalse(SchemaEquality.same(SCHEMA, nestedWithBaseField));
    assertFalse(SchemaEquality.same(nestedWithBaseField, SCHEMA));
  }

  @Test
  void differsOnMissingOrExtraField() {
    final StructType missing =
        StructType.fromDDL(
            "id STRING, name ARRAY<STRUCT<family: STRING>>, tags MAP<STRING, STRUCT<code:"
                + " STRING>>");
    final StructType extra =
        StructType.fromDDL(
            "id STRING, name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,"
                + " tags MAP<STRING, STRUCT<code: STRING, system: STRING>>");

    assertFalse(SchemaEquality.same(SCHEMA, missing));
    assertFalse(SchemaEquality.same(missing, SCHEMA));
    assertFalse(SchemaEquality.same(SCHEMA, extra));
    assertFalse(SchemaEquality.same(extra, SCHEMA));
  }

  @Test
  void differsOnRenamedOrRetypedField() {
    final StructType renamed =
        StructType.fromDDL(
            "id STRING, name ARRAY<STRUCT<family: STRING, givenName: ARRAY<STRING>>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");
    final StructType retyped =
        StructType.fromDDL(
            "id STRING, name ARRAY<STRUCT<family: STRING, given: STRING>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");
    final StructType retypedLeaf =
        StructType.fromDDL(
            "id STRING, name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,"
                + " tags MAP<STRING, STRUCT<code: INT>>");

    assertFalse(SchemaEquality.same(SCHEMA, renamed));
    assertFalse(SchemaEquality.same(SCHEMA, retyped));
    assertFalse(SchemaEquality.same(SCHEMA, retypedLeaf));
  }

  @Test
  void differsOnFieldOrder() {
    final StructType topLevel =
        StructType.fromDDL(
            "name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>, id STRING,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");
    final StructType nested =
        StructType.fromDDL(
            "id STRING, name ARRAY<STRUCT<given: ARRAY<STRING>, family: STRING>>,"
                + " tags MAP<STRING, STRUCT<code: STRING>>");

    assertFalse(SchemaEquality.same(SCHEMA, topLevel));
    assertFalse(SchemaEquality.same(SCHEMA, nested));
  }

  @Test
  void schemaSetsAreTheSameWhenTypesAndSchemasAre() {
    final StructType observation = StructType.fromDDL("id STRING, status STRING");

    assertTrue(
        SchemaEquality.sameSet(
            Map.of("Patient", SCHEMA, "Observation", observation),
            Map.of("Patient", strict(), "Observation", observation)));
    assertFalse(
        SchemaEquality.sameSet(
            Map.of("Patient", SCHEMA, "Observation", observation), Map.of("Patient", SCHEMA)));
    assertFalse(SchemaEquality.sameSet(Map.of("Patient", SCHEMA), Map.of("Patient", observation)));
    assertFalse(SchemaEquality.sameSet(Map.of("Patient", SCHEMA), Map.of("Condition", SCHEMA)));
  }
}

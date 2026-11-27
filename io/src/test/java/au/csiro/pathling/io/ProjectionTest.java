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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Tests the by-name projection of a dataset onto a target schema. */
class ProjectionTest {

  @Nonnull
  private static StructField field(@Nonnull final String name, @Nonnull final DataType type) {
    return DataTypes.createStructField(name, type, true);
  }

  @Nonnull
  private static StructField required(@Nonnull final String name, @Nonnull final DataType type) {
    return DataTypes.createStructField(name, type, false);
  }

  @Nonnull
  private static StructType struct(@Nonnull final StructField... fields) {
    return DataTypes.createStructType(fields);
  }

  @Nonnull
  private static Dataset<Row> dataset(
      @Nonnull final StructType schema, @Nonnull final Row... rows) {
    return TransformFixtures.spark().createDataFrame(List.of(rows), schema);
  }

  @Nonnull
  private static Row project(@Nonnull final Dataset<Row> source, @Nonnull final StructType target) {
    final Dataset<Row> projected = Projection.project(source, target);
    assertEquals(target, projected.schema());
    return projected.collectAsList().get(0);
  }

  /**
   * Asserts that projecting a dataset of the source schema onto the target fails when the
   * projection is built, before any action, with a message naming each of the paths, delimited by
   * backticks so that a path is not matched inside a longer name.
   */
  private static void assertRefused(
      @Nonnull final StructType source,
      @Nonnull final StructType target,
      @Nonnull final String... paths) {
    final Dataset<Row> dataset = dataset(source);
    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> Projection.project(dataset, target));
    for (final String path : paths) {
      assertTrue(
          error.getMessage().contains("`" + path + "`"),
          "Expected the message to name `" + path + "`: " + error.getMessage());
    }
  }

  @Test
  void dropsExtraAndAddsAbsentFieldsAtTopLevel() {
    final StructType source =
        struct(field("id", DataTypes.StringType), field("extra", DataTypes.IntegerType));
    final StructType target =
        struct(field("id", DataTypes.StringType), field("absent", DataTypes.BooleanType));

    final Row row = project(dataset(source, RowFactory.create("p1", 7)), target);

    assertEquals("p1", row.getString(0));
    assertNull(row.get(1));
  }

  @Test
  void dropsExtraAndAddsAbsentFieldsInsideStruct() {
    final StructType source =
        struct(
            field(
                "name",
                struct(field("family", DataTypes.StringType), field("extra", DataTypes.LongType))));
    final StructType target =
        struct(
            field(
                "name",
                struct(
                    field("family", DataTypes.StringType),
                    field("absent", struct(field("value", DataTypes.StringType))))));

    final Row row =
        project(dataset(source, RowFactory.create(RowFactory.create("Smith", 1L))), target);

    final Row name = row.getStruct(0);
    assertEquals("Smith", name.getString(0));
    assertNull(name.get(1));
  }

  @Test
  void dropsExtraAndAddsAbsentFieldsInsideArray() {
    final StructType source =
        struct(
            field(
                "given",
                DataTypes.createArrayType(
                    struct(
                        field("value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)))));
    final StructType target =
        struct(
            field(
                "given",
                DataTypes.createArrayType(
                    struct(
                        field("value", DataTypes.StringType),
                        field("absent", DataTypes.DateType)))));

    final Row row =
        project(
            dataset(
                source,
                RowFactory.create(List.of(RowFactory.create("a", 1), RowFactory.create("b", 2)))),
            target);

    final List<Row> given = row.getList(0);
    assertEquals(2, given.size());
    assertEquals("a", given.get(0).getString(0));
    assertNull(given.get(0).get(1));
    assertEquals("b", given.get(1).getString(0));
    assertNull(given.get(1).get(1));
  }

  @Test
  void dropsExtraAndAddsAbsentFieldsInsideMap() {
    final StructType source =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(
                        field("value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)))));
    final StructType target =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(
                        field("value", DataTypes.StringType),
                        field("absent", DataTypes.DoubleType)))));

    final Row row =
        project(dataset(source, RowFactory.create(Map.of("k", RowFactory.create("v", 3)))), target);

    final Row value = row.<String, Row>getJavaMap(0).get("k");
    assertEquals("v", value.getString(0));
    assertNull(value.get(1));
  }

  @Test
  void keepsNullStructNull() {
    final StructType source =
        struct(
            field("id", DataTypes.StringType),
            field("name", struct(field("family", DataTypes.StringType))));
    final StructType target =
        struct(
            field("id", DataTypes.StringType),
            field(
                "name",
                struct(
                    field("family", DataTypes.StringType), field("absent", DataTypes.StringType))));

    final Row row = project(dataset(source, RowFactory.create("p1", null)), target);

    assertNull(row.get(1));
  }

  @Test
  void followsTargetFieldOrder() {
    final StructType source =
        struct(
            field("a", DataTypes.StringType),
            field("b", DataTypes.StringType),
            field(
                "nested",
                struct(field("x", DataTypes.StringType), field("y", DataTypes.StringType))));
    final StructType target =
        struct(
            field(
                "nested",
                struct(field("y", DataTypes.StringType), field("x", DataTypes.StringType))),
            field("b", DataTypes.StringType),
            field("a", DataTypes.StringType));

    final Row row =
        project(
            dataset(source, RowFactory.create("va", "vb", RowFactory.create("vx", "vy"))), target);

    assertEquals("vy", row.getStruct(0).getString(0));
    assertEquals("vx", row.getStruct(0).getString(1));
    assertEquals("vb", row.getString(1));
    assertEquals("va", row.getString(2));
  }

  /**
   * Pins that a top-level field whose stored name differs from the target's only in case is
   * refused, naming both names, rather than read as an absent field and nulled (spec edge case "A
   * stored field whose name differs from the defined schema's only in case").
   */
  @Test
  void refusesCaseOnlyNameDifferenceAtTopLevel() {
    final StructType source =
        struct(field("id", DataTypes.StringType), field("Gender", DataTypes.StringType));
    final StructType target =
        struct(field("id", DataTypes.StringType), field("gender", DataTypes.StringType));

    assertRefused(source, target, "Gender", "gender");
  }

  /**
   * Pins that a case-only difference inside a struct is refused even where the subtrees are
   * otherwise equal, a case that Spark's case-insensitive type comparison would pass through under
   * the stored name (spec edge case "A stored field whose name differs from the defined schema's
   * only in case").
   */
  @Test
  void refusesCaseOnlyNameDifferenceInsideOtherwiseEqualStruct() {
    final StructType source = struct(field("name", struct(field("Family", DataTypes.StringType))));
    final StructType target = struct(field("name", struct(field("family", DataTypes.StringType))));

    assertRefused(source, target, "name.Family", "name.family");
  }

  /**
   * Pins that a case-only difference inside a struct that is rebuilt is refused, rather than the
   * stored field being dropped and the target field nulled (spec edge case "A stored field whose
   * name differs from the defined schema's only in case").
   */
  @Test
  void refusesCaseOnlyNameDifferenceInsideRebuiltStruct() {
    final StructType source =
        struct(
            field(
                "name",
                struct(field("Family", DataTypes.StringType), field("extra", DataTypes.LongType))));
    final StructType target = struct(field("name", struct(field("family", DataTypes.StringType))));

    assertRefused(source, target, "name.Family", "name.family");
  }

  /**
   * Pins that a case-only difference inside an array of structs is refused, with the array
   * transparent in the paths (spec edge case "A stored field whose name differs from the defined
   * schema's only in case").
   */
  @Test
  void refusesCaseOnlyNameDifferenceInsideArray() {
    final StructType source =
        struct(
            field(
                "given",
                DataTypes.createArrayType(
                    struct(
                        field("Value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)))));
    final StructType target =
        struct(
            field(
                "given", DataTypes.createArrayType(struct(field("value", DataTypes.StringType)))));

    assertRefused(source, target, "given.Value", "given.value");
  }

  /**
   * Pins that a stored struct holding two top-level fields whose names differ only in case is
   * refused, naming both, rather than failing on an ambiguous reference (spec edge case "A stored
   * field whose name differs from the defined schema's only in case").
   */
  @Test
  void refusesFieldsDifferingOnlyInCaseAtTopLevel() {
    final StructType source =
        struct(field("gender", DataTypes.StringType), field("Gender", DataTypes.StringType));
    final StructType target = struct(field("gender", DataTypes.StringType));

    assertRefused(source, target, "gender", "Gender");
  }

  /**
   * Pins that a stored nested struct holding two fields whose names differ only in case is refused,
   * naming both paths, rather than failing on an ambiguous reference (spec edge case "A stored
   * field whose name differs from the defined schema's only in case").
   */
  @Test
  void refusesFieldsDifferingOnlyInCaseInsideStruct() {
    final StructType source =
        struct(
            field(
                "name",
                struct(
                    field("family", DataTypes.StringType), field("Family", DataTypes.StringType))));
    final StructType target =
        struct(
            field(
                "name",
                struct(
                    field("family", DataTypes.StringType), field("absent", DataTypes.StringType))));

    assertRefused(source, target, "name.family", "name.Family");
  }

  /**
   * Pins that a struct held as a map value with two fields whose names differ only in case is
   * refused, with the map transparent in the paths.
   */
  @Test
  void refusesFieldsDifferingOnlyInCaseInsideMapValue() {
    final StructType source =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(
                        field("value", DataTypes.StringType),
                        field("Value", DataTypes.StringType)))));
    final StructType target =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(
                        field("value", DataTypes.StringType),
                        field("absent", DataTypes.StringType)))));

    assertRefused(source, target, "byKey.value", "byKey.Value");
  }

  /**
   * Pins the pass-through that R14 relies on: a subtree whose type equals the target's apart from
   * nullability is not rebuilt, so the projection of a canonical schema is a no-op. The stored
   * nullability surviving, and the plan holding no rebuilt struct, both show it.
   */
  @Test
  void passesThroughSubtreeEqualToTargetIgnoringNullability() {
    final StructType source =
        struct(
            field(
                "name", struct(DataTypes.createStructField("family", DataTypes.StringType, false))),
            field("extra", DataTypes.IntegerType));
    final StructType target = struct(field("name", struct(field("family", DataTypes.StringType))));

    final Dataset<Row> projected =
        Projection.project(
            dataset(source, RowFactory.create(RowFactory.create("Smith"), 1)), target);

    assertEquals(struct(field("name", source.apply("name").dataType())), projected.schema());
    assertFalse(projected.queryExecution().analyzed().toString().contains("named_struct"));
    assertEquals("Smith", projected.collectAsList().get(0).getStruct(0).getString(0));
  }

  /**
   * Pins that a null array stays null when its elements are rebuilt, rather than becoming an empty
   * array.
   */
  @Test
  void keepsNullArrayNull() {
    final StructType source =
        struct(
            field(
                "given",
                DataTypes.createArrayType(
                    struct(
                        field("value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)))));
    final StructType target =
        struct(
            field(
                "given", DataTypes.createArrayType(struct(field("value", DataTypes.StringType)))));

    final Row row = project(dataset(source, RowFactory.create((Object) null)), target);

    assertTrue(row.isNullAt(0));
  }

  /**
   * Pins that a null map stays null when its values are rebuilt, rather than becoming an empty map.
   */
  @Test
  void keepsNullMapNull() {
    final StructType source =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(
                        field("value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)))));
    final StructType target =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType, struct(field("value", DataTypes.StringType)))));

    final Row row = project(dataset(source, RowFactory.create((Object) null)), target);

    assertTrue(row.isNullAt(0));
  }

  /**
   * Pins that structs inside an array that differ from the target only in field order are reordered
   * to the target's order with their values kept (R14).
   */
  @Test
  void followsTargetFieldOrderInsideArray() {
    final StructType source =
        struct(
            field(
                "items",
                DataTypes.createArrayType(
                    struct(field("x", DataTypes.StringType), field("y", DataTypes.StringType)))));
    final StructType target =
        struct(
            field(
                "items",
                DataTypes.createArrayType(
                    struct(field("y", DataTypes.StringType), field("x", DataTypes.StringType)))));

    final Row row =
        project(
            dataset(
                source,
                RowFactory.create(
                    List.of(RowFactory.create("x1", "y1"), RowFactory.create("x2", "y2")))),
            target);

    final List<Row> items = row.getList(0);
    assertEquals(2, items.size());
    assertEquals("y1", items.get(0).getString(0));
    assertEquals("x1", items.get(0).getString(1));
    assertEquals("y2", items.get(1).getString(0));
    assertEquals("x2", items.get(1).getString(1));
  }

  /**
   * Pins that structs held as map values that differ from the target only in field order are
   * reordered to the target's order with their values kept (R14).
   */
  @Test
  void followsTargetFieldOrderInsideMap() {
    final StructType source =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(field("x", DataTypes.StringType), field("y", DataTypes.StringType)))));
    final StructType target =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(field("y", DataTypes.StringType), field("x", DataTypes.StringType)))));

    final Row row =
        project(
            dataset(source, RowFactory.create(Map.of("k", RowFactory.create("vx", "vy")))), target);

    final Row value = row.<String, Row>getJavaMap(0).get("k");
    assertEquals("vy", value.getString(0));
    assertEquals("vx", value.getString(1));
  }

  /**
   * Pins that a struct that cannot be null keeps its flags when the target narrows it, along with
   * the flags of the fields inside it, and that a struct inside it that cannot be null and is
   * narrowed too does the same, whatever the target declares.
   */
  @Test
  void keepsFlagsOfNarrowedNotNullStruct() {
    final StructType source =
        struct(
            required("id", DataTypes.StringType),
            required(
                "name",
                struct(
                    field("family", DataTypes.StringType),
                    required("given", DataTypes.StringType),
                    required(
                        "period",
                        struct(
                            required("start", DataTypes.StringType),
                            field("extra", DataTypes.LongType))),
                    field("extra", DataTypes.LongType))));
    final StructType target =
        struct(
            field("id", DataTypes.StringType),
            field(
                "name",
                struct(
                    field("family", DataTypes.StringType),
                    field("given", DataTypes.StringType),
                    field("period", struct(field("start", DataTypes.StringType))))));

    final Dataset<Row> projected =
        Projection.project(
            dataset(
                source,
                RowFactory.create(
                    "p1", RowFactory.create(null, "John", RowFactory.create("2020", 1L), 2L))),
            target);

    assertEquals(
        struct(
            required("id", DataTypes.StringType),
            required(
                "name",
                struct(
                    field("family", DataTypes.StringType),
                    required("given", DataTypes.StringType),
                    required("period", struct(required("start", DataTypes.StringType)))))),
        projected.schema());
    final Row row = projected.collectAsList().get(0);
    assertEquals("p1", row.getString(0));
    final Row name = row.getStruct(1);
    assertTrue(name.isNullAt(0));
    assertEquals("John", name.getString(1));
    assertEquals("2020", name.getStruct(2).getString(0));
  }

  /**
   * Pins that the elements of an array that cannot hold null stay non-null when the target narrows
   * them, so the array keeps {@code containsNull=false}.
   */
  @Test
  void keepsFlagsOfNarrowedArrayOfNotNullStructs() {
    final StructType source =
        struct(
            field(
                "items",
                DataTypes.createArrayType(
                    struct(
                        required("value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)),
                    false)));
    final StructType target =
        struct(
            field(
                "items", DataTypes.createArrayType(struct(field("value", DataTypes.StringType)))));

    final Dataset<Row> projected =
        Projection.project(
            dataset(
                source,
                RowFactory.create(List.of(RowFactory.create("a", 1), RowFactory.create("b", 2)))),
            target);

    assertEquals(
        struct(
            field(
                "items",
                DataTypes.createArrayType(struct(required("value", DataTypes.StringType)), false))),
        projected.schema());
    final List<Row> items = projected.collectAsList().get(0).getList(0);
    assertEquals(2, items.size());
    assertEquals("a", items.get(0).getString(0));
    assertEquals("b", items.get(1).getString(0));
  }

  /**
   * Pins that the values of a map that cannot hold null stay non-null when the target narrows them,
   * so the map keeps {@code valueContainsNull=false}.
   */
  @Test
  void keepsFlagsOfNarrowedMapOfNotNullStructs() {
    final StructType source =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    struct(
                        required("value", DataTypes.StringType),
                        field("extra", DataTypes.IntegerType)),
                    false)));
    final StructType target =
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType, struct(field("value", DataTypes.StringType)))));

    final Dataset<Row> projected =
        Projection.project(
            dataset(source, RowFactory.create(Map.of("k", RowFactory.create("v", 1)))), target);

    assertEquals(
        struct(
            field(
                "byKey",
                DataTypes.createMapType(
                    DataTypes.StringType, struct(required("value", DataTypes.StringType)), false))),
        projected.schema());
    final Row value = projected.collectAsList().get(0).<String, Row>getJavaMap(0).get("k");
    assertEquals("v", value.getString(0));
  }

  /**
   * Pins that a struct that can be null stays nullable when the target narrows it, and that a null
   * value of it is restored as null rather than as a struct full of nulls, even where the target
   * declares it not nullable. A field inside it that cannot be null comes out nullable, because
   * Spark makes any field reached through a nullable struct nullable, and the struct is rebuilt
   * from such fields.
   */
  @Test
  void keepsFlagsAndNullOfNarrowedNullableStruct() {
    final StructType source =
        struct(
            required("id", DataTypes.StringType),
            field(
                "name",
                struct(
                    required("family", DataTypes.StringType), field("extra", DataTypes.LongType))));
    final StructType target =
        struct(
            required("id", DataTypes.StringType),
            required("name", struct(field("family", DataTypes.StringType))));

    final Dataset<Row> projected =
        Projection.project(
            dataset(
                source,
                RowFactory.create("p1", null),
                RowFactory.create("p2", RowFactory.create("Smith", 1L))),
            target);

    assertEquals(
        struct(
            required("id", DataTypes.StringType),
            field("name", struct(field("family", DataTypes.StringType)))),
        projected.schema());
    final List<Row> rows = projected.collectAsList();
    assertTrue(rows.get(0).isNullAt(1));
    assertEquals("Smith", rows.get(1).getStruct(1).getString(0));
  }

  /**
   * Pins that a field only the target has is nullable, at the top level and inside a struct that
   * cannot be null, even where the target declares it not nullable.
   */
  @Test
  void suppliesFieldOnlyTargetHasAsNullable() {
    final StructType source =
        struct(
            required("id", DataTypes.StringType),
            required("name", struct(required("family", DataTypes.StringType))));
    final StructType target =
        struct(
            required("id", DataTypes.StringType),
            required("absent", DataTypes.StringType),
            required(
                "name",
                struct(
                    required("family", DataTypes.StringType),
                    required("absent", struct(required("value", DataTypes.StringType))))));

    final Dataset<Row> projected =
        Projection.project(
            dataset(source, RowFactory.create("p1", RowFactory.create("Smith"))), target);

    assertEquals(
        struct(
            required("id", DataTypes.StringType),
            field("absent", DataTypes.StringType),
            required(
                "name",
                struct(
                    required("family", DataTypes.StringType),
                    field("absent", struct(required("value", DataTypes.StringType)))))),
        projected.schema());
    final Row row = projected.collectAsList().get(0);
    assertEquals("p1", row.getString(0));
    assertTrue(row.isNullAt(1));
    assertEquals("Smith", row.getStruct(2).getString(0));
    assertTrue(row.getStruct(2).isNullAt(1));
  }
}

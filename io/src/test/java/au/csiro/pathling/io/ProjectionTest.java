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
import static org.junit.jupiter.api.Assertions.assertNull;

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
}

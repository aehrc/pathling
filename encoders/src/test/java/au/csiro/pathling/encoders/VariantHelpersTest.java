/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for Variant wrapping and unwrapping helpers in {@link ValueFunctions}.
 *
 * <p>Tests the round-trip conversion of struct arrays through Spark's Variant type, covering normal
 * arrays, empty arrays, and null elements.
 */
class VariantHelpersTest {

  private static SparkSession spark;

  /** Schema for a simple struct with name and value fields. */
  private static final StructType ITEM_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("value", DataTypes.IntegerType, true)
          });

  /** Schema for an outer struct containing an array of items. */
  private static final StructType ROW_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("items", DataTypes.createArrayType(ITEM_SCHEMA, true), true)
          });

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("testing")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  private Dataset<Row> createDataset(final Row... rows) {
    return spark.createDataFrame(List.of(rows), ROW_SCHEMA);
  }

  @Test
  void wrapWithVariantConvertsStructArrayToVariantArray() {
    // Create a dataset with a struct array column.
    final Dataset<Row> ds =
        createDataset(RowFactory.create((Object) new Row[] {RowFactory.create("a", 1)}));

    // Wrap the identity transform with Variant conversion.
    final UnaryOperator<Column> wrapped = ValueFunctions.wrapWithVariant(c -> c);
    final Dataset<Row> result = ds.select(wrapped.apply(functions.col("items")).alias("result"));

    // The result should be a non-null array with one element.
    final Row row = result.head();
    final List<?> resultItems = row.getList(0);
    assertEquals(1, resultItems.size());
    assertNotNull(resultItems.get(0));
  }

  @Test
  void unwrapVariantArrayRoundTripsStructData() {
    // Create a dataset with a struct array, convert to Variant, and convert back.
    final Dataset<Row> ds =
        createDataset(
            RowFactory.create(
                (Object) new Row[] {RowFactory.create("a", 1), RowFactory.create("b", 2)}));

    // Convert to Variant array and back.
    final Column items = functions.col("items");
    final Column variantArray = functions.transform(items, functions::to_variant_object);
    final Column unwrapped = ValueFunctions.unwrapVariantArray(variantArray, ITEM_SCHEMA);
    final Dataset<Row> result = ds.select(unwrapped.alias("result"));

    // Verify the round-trip preserves data.
    final Row row = result.head();
    final List<Row> resultItems = row.getList(0);
    assertEquals(2, resultItems.size());
    assertEquals("a", resultItems.get(0).getString(0));
    assertEquals(1, resultItems.get(0).getInt(1));
    assertEquals("b", resultItems.get(1).getString(0));
    assertEquals(2, resultItems.get(1).getInt(1));
  }

  @Test
  void unwrapVariantArrayHandlesEmptyArray() {
    // Create a dataset with an empty struct array.
    final Dataset<Row> ds = createDataset(RowFactory.create((Object) new Row[] {}));

    // Convert to Variant array and back.
    final Column items = functions.col("items");
    final Column variantArray = functions.transform(items, functions::to_variant_object);
    final Column unwrapped = ValueFunctions.unwrapVariantArray(variantArray, ITEM_SCHEMA);
    final Dataset<Row> result = ds.select(unwrapped.alias("result"));

    // Verify the round-trip returns an empty array.
    final Row row = result.head();
    final List<Row> resultItems = row.getList(0);
    assertEquals(0, resultItems.size());
  }

  @Test
  void unwrapVariantArrayHandlesNullArray() {
    // Create a dataset with a null items column.
    final Dataset<Row> ds = createDataset(RowFactory.create((Object) null));

    // Convert to Variant array and back.
    final Column items = functions.col("items");
    final Column variantArray = functions.transform(items, functions::to_variant_object);
    final Column unwrapped = ValueFunctions.unwrapVariantArray(variantArray, ITEM_SCHEMA);
    final Dataset<Row> result = ds.select(unwrapped.alias("result"));

    // Verify the round-trip returns null.
    final Row row = result.head();
    assertNull(row.get(0));
  }

  @Test
  void unwrapVariantArrayFillsMissingFieldsWithNull() {
    // Create a dataset with structs that have only the 'name' field, simulating schema divergence
    // where deeper nesting levels lack certain fields.
    final StructType partialSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)});
    final StructType partialRowSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "items", DataTypes.createArrayType(partialSchema, true), true)
            });

    final Dataset<Row> ds =
        spark.createDataFrame(
            List.of(RowFactory.create((Object) new Row[] {RowFactory.create("partial")})),
            partialRowSchema);

    // Convert to Variant array and unwrap with the full target schema.
    final Column items = functions.col("items");
    final Column variantArray = functions.transform(items, functions::to_variant_object);
    final Column unwrapped = ValueFunctions.unwrapVariantArray(variantArray, ITEM_SCHEMA);
    final Dataset<Row> result = ds.select(unwrapped.alias("result"));

    // Verify the missing 'value' field is filled with null.
    final Row row = result.head();
    final List<Row> resultItems = row.getList(0);
    assertEquals(1, resultItems.size());
    assertEquals("partial", resultItems.get(0).getString(0));
    assertNull(resultItems.get(0).get(1));
  }
}

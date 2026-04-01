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

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.inline;
import static org.apache.spark.sql.functions.struct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Abstract base class for expression tests that need to run in both codegen and interpreted modes.
 * Subclasses configure the Spark session and annotate their own setup/teardown methods.
 */
public abstract class ExpressionsBothModesTest {

  protected static SparkSession spark;

  /** Creates the shared Spark session. Subclasses should call this from their @BeforeAll method. */
  protected static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("testing")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @Test
  void testArrayCrossProd() {
    final Dataset<Row> ds = spark.range(1).toDF();

    // Create three input array columns.
    final Dataset<Row> inputDs =
        ds.withColumn(
                "arrayA",
                functions.array(
                    functions.struct(
                        functions.lit("zzz").alias("str"), functions.lit(true).alias("bool")),
                    functions.struct(
                        functions.lit("yyy").alias("str"), functions.lit(false).alias("bool"))))
            .withColumn(
                "arrayB",
                functions.array(
                    functions.struct(functions.lit(13).alias("int")),
                    functions.struct(functions.lit(17).alias("int")),
                    functions.struct(functions.lit(1).alias("int"))))
            .withColumn(
                "arrayC",
                functions.array(
                    functions.struct(functions.lit(13.1).alias("float")),
                    functions.struct(functions.lit(17.2).alias("float"))));

    // Test structProduct.
    final Dataset<Row> structProductDs =
        inputDs
            .withColumn("sp_one", ColumnFunctions.structProduct(inputDs.col("arrayA")))
            .withColumn(
                "sp_two",
                ColumnFunctions.structProduct(inputDs.col("arrayA"), inputDs.col("arrayB")))
            .withColumn(
                "sp_three",
                ColumnFunctions.structProduct(
                    inputDs.col("arrayA"), inputDs.col("arrayB"), inputDs.col("arrayC")));

    // Test structProductOuter.
    final Dataset<Row> structProductOuterDs =
        inputDs
            .withColumn("spo_one", ColumnFunctions.structProductOuter(inputDs.col("arrayA")))
            .withColumn(
                "spo_two",
                ColumnFunctions.structProductOuter(inputDs.col("arrayA"), inputDs.col("arrayB")))
            .withColumn(
                "spo_three",
                ColumnFunctions.structProductOuter(
                    inputDs.col("arrayA"), inputDs.col("arrayB"), inputDs.col("arrayC")));

    // Collect and assert structProduct results.
    final List<Row> spResults = structProductDs.collectAsList();
    assertEquals(1, spResults.size());
    final Row spRow = spResults.getFirst();
    assertNotNull(spRow.getAs("sp_one"));
    assertNotNull(spRow.getAs("sp_two"));
    assertNotNull(spRow.getAs("sp_three"));

    // Collect and assert structProductOuter results.
    final List<Row> spoResults = structProductOuterDs.collectAsList();
    assertEquals(1, spoResults.size());
    final Row spoRow = spoResults.getFirst();
    assertNotNull(spoRow.getAs("spo_one"));
    assertNotNull(spoRow.getAs("spo_two"));
    assertNotNull(spoRow.getAs("spo_three"));

    // Assert sizes against expected product.
    final int aSize = 2; // arrayA
    final int bSize = 3; // arrayB
    final int cSize = 2; // arrayC

    assertEquals(aSize, ((Seq<?>) spRow.getAs("sp_one")).size());
    assertEquals(aSize, ((Seq<?>) spoRow.getAs("spo_one")).size());

    assertEquals(aSize * bSize, ((Seq<?>) spRow.getAs("sp_two")).size());
    assertEquals(aSize * bSize, ((Seq<?>) spoRow.getAs("spo_two")).size());

    assertEquals(aSize * bSize * cSize, ((Seq<?>) spRow.getAs("sp_three")).size());
    assertEquals(aSize * bSize * cSize, ((Seq<?>) spoRow.getAs("spo_three")).size());
  }

  @Test
  void testArrayCrossProdWithNullsAndEmptys() {
    final Dataset<Row> ds = spark.range(1).toDF();

    // Create input array columns.
    final Dataset<Row> inputDs =
        ds.withColumn(
                "nullArray",
                functions
                    .lit(null)
                    .cast(
                        DataTypes.createArrayType(
                            DataTypes.createStructType(
                                Arrays.asList(
                                    DataTypes.createStructField("str", DataTypes.StringType, true),
                                    DataTypes.createStructField(
                                        "int", DataTypes.IntegerType, true))))))
            .withColumn(
                "emptyArray",
                functions
                    .array()
                    .cast(
                        DataTypes.createArrayType(
                            DataTypes.createStructType(
                                Arrays.asList(
                                    DataTypes.createStructField("str", DataTypes.StringType, true),
                                    DataTypes.createStructField(
                                        "int", DataTypes.IntegerType, true))))))
            .withColumn(
                "nonEmptyArray",
                functions.array(
                    functions.struct(
                        functions.lit("zzz").alias("str"), functions.lit(1).alias("int")),
                    functions.struct(
                        functions.lit("yyy").alias("str"), functions.lit(2).alias("int"))));

    // Test structProduct.
    final Dataset<Row> structProductDs =
        inputDs
            .withColumn("sp_null", ColumnFunctions.structProduct(inputDs.col("nullArray")))
            .withColumn("sp_empty", ColumnFunctions.structProduct(inputDs.col("emptyArray")))
            .withColumn(
                "sp_nonEmpty",
                ColumnFunctions.structProduct(
                    inputDs.col("nonEmptyArray"), inputDs.col("emptyArray")));

    // Test structProductOuter.
    final Dataset<Row> structProductOuterDs =
        inputDs
            .withColumn("spo_null", ColumnFunctions.structProductOuter(inputDs.col("nullArray")))
            .withColumn("spo_empty", ColumnFunctions.structProductOuter(inputDs.col("emptyArray")))
            .withColumn(
                "spo_nonEmpty",
                ColumnFunctions.structProductOuter(
                    inputDs.col("nonEmptyArray"), inputDs.col("emptyArray")));

    // Collect and assert structProduct results.
    final List<Row> spResults = structProductDs.collectAsList();
    assertEquals(1, spResults.size());
    final Row spRow = spResults.getFirst();
    assertNotNull(spRow.getAs("sp_null"));
    assertEquals(0, ((Seq<?>) spRow.getAs("sp_null")).size());
    assertNotNull(spRow.getAs("sp_empty"));
    assertEquals(0, ((Seq<?>) spRow.getAs("sp_empty")).size());
    assertNotNull(spRow.getAs("sp_nonEmpty"));
    assertEquals(0, ((Seq<?>) spRow.getAs("sp_nonEmpty")).size());

    // Collect and assert structProductOuter results.
    final List<Row> spoResults = structProductOuterDs.collectAsList();
    assertEquals(1, spoResults.size());
    final Row spoRow = spoResults.getFirst();
    assertNotNull(spoRow.getAs("spo_null"));
    assertEquals(1, ((Seq<?>) spoRow.getAs("spo_null")).size());
    assertNotNull(spoRow.getAs("spo_empty"));
    assertEquals(1, ((Seq<?>) spoRow.getAs("spo_empty")).size());
    assertNotNull(spoRow.getAs("spo_nonEmpty"));
    assertEquals(1, ((Seq<?>) spoRow.getAs("spo_nonEmpty")).size());
  }

  /**
   * Regression test for issue #2568: {@code inline(structProduct(...))} crashed with NPE when input
   * data was stored in Spark's Tungsten/unsafe format. The root cause was StructProduct passing
   * {@code null} as the dataType when calling {@code InternalRow.get(ordinal, dataType)} to copy
   * struct fields. This works for {@code GenericInternalRow} (used by {@code
   * functions.array()}/{@code functions.lit()}) but fails for {@code UnsafeRow} (used when data
   * comes from {@code spark.createDataFrame()}).
   */
  @Test
  void testStructProductInlineWithUnsafeRowData() {
    final Metadata metadata = Metadata.empty();

    final StructType codingType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("system", DataTypes.StringType, true, metadata),
              new StructField("code", DataTypes.StringType, true, metadata),
              new StructField("display", DataTypes.StringType, true, metadata)
            });

    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, metadata),
              new StructField("text", DataTypes.StringType, true, metadata),
              new StructField("coding", DataTypes.createArrayType(codingType), true, metadata)
            });

    // Data created via createDataFrame stores struct array elements as UnsafeRow.
    final List<Row> data =
        Arrays.asList(
            RowFactory.create(
                "1",
                "Blood pressure",
                Arrays.asList(
                    RowFactory.create("http://loinc.org", "85354-9", "BP systolic & diastolic"),
                    RowFactory.create("http://snomed.info/sct", "75367002", "Blood pressure"))),
            RowFactory.create(
                "2",
                "Heart rate",
                List.of(RowFactory.create("http://loinc.org", "8867-4", "Heart rate"))));

    final Dataset<Row> ds = spark.createDataFrame(data, schema).repartition(1);

    // Build the struct product: cross product of [id], [text], and coding[].
    final Column idArray = array(struct(col("id").alias("id")));
    final Column textArray = array(struct(col("text").alias("text")));
    final Column codingArray = col("coding");
    final Column product = ColumnFunctions.structProduct(idArray, textArray, codingArray);

    // Inline the struct product and collect — this crashed with NPE before the fix.
    final Dataset<Row> result = ds.select(inline(product));
    final List<Row> actual = result.collectAsList();

    final List<Row> expected =
        Arrays.asList(
            RowFactory.create(
                "1", "Blood pressure", "http://loinc.org", "85354-9", "BP systolic & diastolic"),
            RowFactory.create(
                "1", "Blood pressure", "http://snomed.info/sct", "75367002", "Blood pressure"),
            RowFactory.create("2", "Heart rate", "http://loinc.org", "8867-4", "Heart rate"));

    assertEquals(expected.size(), actual.size(), "Row count mismatch");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i), "Row " + i + " mismatch");
    }
  }

  /**
   * Tests that RowCounter produces sequential 0-based indices within a simple array transform, and
   * that ResetCounter resets the sequence for each row.
   */
  @Test
  void testRowCounterWithSimpleTransform() {
    final RowIndexCounter counter = new RowIndexCounter();
    final Column counterCol = ValueFunctions.rowCounter(counter);

    // Create a dataset with two rows, each containing an array of different lengths.
    final Dataset<Row> ds =
        spark
            .createDataFrame(
                List.of(
                    RowFactory.create(1, List.of("a", "b", "c")),
                    RowFactory.create(2, List.of("d", "e"))),
                DataTypes.createStructType(
                    new StructField[] {
                      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                      new StructField(
                          "items",
                          DataTypes.createArrayType(DataTypes.StringType),
                          false,
                          Metadata.empty())
                    }))
            .repartition(1);

    // Use transform to stamp each element with the counter, then wrap with resetCounter.
    final Column transformed =
        functions.transform(
            ds.col("items"), elem -> functions.struct(elem.alias("val"), counterCol.alias("idx")));
    final Column withReset = ValueFunctions.resetCounter(transformed, counter);

    final Dataset<Row> result = ds.withColumn("indexed", withReset);
    final List<Row> rows = result.collectAsList();

    assertEquals(2, rows.size());

    // Row 1: 3 elements → indices 0, 1, 2.
    final Seq<?> row1Seq = rows.get(0).getAs("indexed");
    final List<?> row1Items = CollectionConverters.asJava(row1Seq);
    assertEquals(3, row1Items.size());
    assertEquals(0, (int) ((Row) row1Items.get(0)).getAs("idx"));
    assertEquals(1, (int) ((Row) row1Items.get(1)).getAs("idx"));
    assertEquals(2, (int) ((Row) row1Items.get(2)).getAs("idx"));

    // Row 2: 2 elements → indices reset to 0, 1.
    final Seq<?> row2Seq = rows.get(1).getAs("indexed");
    final List<?> row2Items = CollectionConverters.asJava(row2Seq);
    assertEquals(2, row2Items.size());
    assertEquals(0, (int) ((Row) row2Items.get(0)).getAs("idx"));
    assertEquals(1, (int) ((Row) row2Items.get(1)).getAs("idx"));
  }

  /**
   * Tests that RowCounter produces a continuous global sequence when used inside a transformTree
   * with a single traversal, producing sequential indices across all depth levels.
   */
  @Test
  void testRowCounterWithTransformTree() {
    final Metadata metadata = Metadata.empty();

    // Build a 3-level nested structure: root has 2 items, first item has 1 child.
    final StructType leafType =
        DataTypes.createStructType(
            new StructField[] {new StructField("linkId", DataTypes.StringType, true, metadata)});

    final StructType midType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("item", DataTypes.createArrayType(leafType), true, metadata)
            });

    final StructType rootItemType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("item", DataTypes.createArrayType(midType), true, metadata)
            });

    final StructType rootSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, metadata),
              new StructField("items", DataTypes.createArrayType(rootItemType), true, metadata)
            });

    // Tree structure:
    //   items[0] (linkId: "1")
    //     └── item[0] (linkId: "1.1")
    //           └── item[0] (linkId: "1.1.1")
    //   items[1] (linkId: "2")
    final Row leaf = RowFactory.create("1.1.1");
    final Row mid = RowFactory.create("1.1", List.of(leaf));
    final Row root0 = RowFactory.create("1", List.of(mid));
    final Row root1 = RowFactory.create("2", List.of());

    final Dataset<Row> ds =
        spark
            .createDataFrame(
                List.of(
                    RowFactory.create(1, List.of(root0, root1)),
                    RowFactory.create(2, List.of(root1))),
                rootSchema)
            .repartition(1);

    final RowIndexCounter counter = new RowIndexCounter();
    final Column counterCol = ValueFunctions.rowCounter(counter);

    // Extractor: produce Array[Struct{linkId, idx}] from each array node.
    final Column treeResult =
        ValueFunctions.transformTree(
            ds.col("items"),
            c ->
                functions.transform(
                    c,
                    elem ->
                        functions.struct(
                            elem.getField("linkId").alias("linkId"), counterCol.alias("idx"))),
            List.of(c -> ValueFunctions.unnest(c.getField("item"))),
            2);

    final Column withReset = ValueFunctions.resetCounter(treeResult, counter);
    final Dataset<Row> result = ds.withColumn("collected", withReset);
    final List<Row> rows = result.collectAsList();

    assertEquals(2, rows.size());

    // Row 1: transformTree produces breadth-first-like order:
    //   Concat(extractor(root_items), transformTree(root_items.item))
    //   = Concat(["1","2"], Concat(["1.1"], ["1.1.1"]))
    //   = ["1", "2", "1.1", "1.1.1"]
    final Seq<?> row1Seq = rows.get(0).getAs("collected");
    final List<?> row1 = CollectionConverters.asJava(row1Seq);
    assertEquals(4, row1.size());
    assertEquals("1", ((Row) row1.get(0)).getAs("linkId"));
    assertEquals(0, (int) ((Row) row1.get(0)).getAs("idx"));
    assertEquals("2", ((Row) row1.get(1)).getAs("linkId"));
    assertEquals(1, (int) ((Row) row1.get(1)).getAs("idx"));
    assertEquals("1.1", ((Row) row1.get(2)).getAs("linkId"));
    assertEquals(2, (int) ((Row) row1.get(2)).getAs("idx"));
    assertEquals("1.1.1", ((Row) row1.get(3)).getAs("linkId"));
    assertEquals(3, (int) ((Row) row1.get(3)).getAs("idx"));

    // Row 2: tree has 1 node → "2"(0) — counter resets.
    final Seq<?> row2Seq = rows.get(1).getAs("collected");
    final List<?> row2 = CollectionConverters.asJava(row2Seq);
    assertEquals(1, row2.size());
    assertEquals("2", ((Row) row2.get(0)).getAs("linkId"));
    assertEquals(0, (int) ((Row) row2.get(0)).getAs("idx"));
  }

  /**
   * Tests that RowCounter works with multiple traversal paths in transformTree, producing a
   * continuous global index across all branches and depths.
   */
  @Test
  void testRowCounterWithMultipleTraversals() {
    final Metadata metadata = Metadata.empty();

    // Build a structure with two traversal paths: "item" and self-reference.
    final StructType level2Type =
        DataTypes.createStructType(
            new StructField[] {new StructField("linkId", DataTypes.StringType, true, metadata)});

    final StructType level1Type =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("item", DataTypes.createArrayType(level2Type), true, metadata)
            });

    final StructType level0Type =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("item", DataTypes.createArrayType(level1Type), true, metadata)
            });

    final StructType rootSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, metadata),
              new StructField("items", DataTypes.createArrayType(level0Type), true, metadata)
            });

    // items[0] (linkId: "1") → item[0] (linkId: "2") → item[0] (linkId: "3").
    final Row level2 = RowFactory.create("3");
    final Row level1 = RowFactory.create("2", List.of(level2));
    final Row level0 = RowFactory.create("1", List.of(level1));

    final Dataset<Row> ds =
        spark
            .createDataFrame(List.of(RowFactory.create(1, List.of(level0))), rootSchema)
            .repartition(1);

    final RowIndexCounter counter = new RowIndexCounter();
    final Column counterCol = ValueFunctions.rowCounter(counter);

    // Use two traversals: item navigation and self-reference (like the existing test).
    final Column treeResult =
        ValueFunctions.transformTree(
            ds.col("items"),
            c ->
                functions.transform(
                    c,
                    elem ->
                        functions.struct(
                            elem.getField("linkId").alias("linkId"), counterCol.alias("idx"))),
            List.of(c -> ValueFunctions.unnest(c.getField("item")), c -> c),
            1);

    final Column withReset = ValueFunctions.resetCounter(treeResult, counter);
    final Dataset<Row> result = ds.withColumn("collected", withReset);
    final List<Row> rows = result.collectAsList();

    assertEquals(1, rows.size());

    // The existing test (without counter) produces linkIds: [1, 2, 3, 3, 2, 3, 1, 2, 3].
    // Each element should have a sequential global index.
    final Seq<?> collectedSeq = rows.get(0).getAs("collected");
    final List<?> collected = CollectionConverters.asJava(collectedSeq);
    assertEquals(9, collected.size());

    // Verify sequential indices 0..8.
    for (int i = 0; i < 9; i++) {
      assertEquals(
          i, (int) ((Row) collected.get(i)).getAs("idx"), "Index mismatch at position " + i);
    }
  }

  /**
   * Tests that RowCounter composes with arithmetic expressions, validating that %rowIndex + 1 style
   * usage works correctly.
   */
  @Test
  void testRowCounterInArithmeticExpression() {
    final RowIndexCounter counter = new RowIndexCounter();
    final Column counterCol = ValueFunctions.rowCounter(counter);

    final Dataset<Row> ds =
        spark
            .createDataFrame(
                List.of(RowFactory.create(1, List.of("a", "b", "c"))),
                DataTypes.createStructType(
                    new StructField[] {
                      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                      new StructField(
                          "items",
                          DataTypes.createArrayType(DataTypes.StringType),
                          false,
                          Metadata.empty())
                    }))
            .repartition(1);

    // Use counter in an arithmetic expression: counter + 1 (1-based index).
    final Column transformed =
        functions.transform(
            ds.col("items"),
            elem -> functions.struct(elem.alias("val"), counterCol.plus(1).alias("one_based_idx")));
    final Column withReset = ValueFunctions.resetCounter(transformed, counter);

    final Dataset<Row> result = ds.withColumn("indexed", withReset);
    final List<Row> rows = result.collectAsList();

    assertEquals(1, rows.size());
    final Seq<?> itemsSeq = rows.get(0).getAs("indexed");
    final List<?> items = CollectionConverters.asJava(itemsSeq);
    assertEquals(3, items.size());
    assertEquals(1, (int) ((Row) items.get(0)).getAs("one_based_idx"));
    assertEquals(2, (int) ((Row) items.get(1)).getAs("one_based_idx"));
    assertEquals(3, (int) ((Row) items.get(2)).getAs("one_based_idx"));
  }
}

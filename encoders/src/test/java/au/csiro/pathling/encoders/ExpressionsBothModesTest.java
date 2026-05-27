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
   * Tests that {@link ValueFunctions#rowCounterGet}, {@link ValueFunctions#rowCounterIncrement},
   * and {@link ValueFunctions#resetCounter} work together to assign sequential indices within an
   * array transform and reset between rows.
   */
  @Test
  void testRowCounterExpressions() {
    // Create a dataset with two rows, each containing an array of structs.
    final StructType itemType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("value", DataTypes.StringType, true, Metadata.empty())
            });
    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, Metadata.empty()),
              new StructField("items", DataTypes.createArrayType(itemType), true, Metadata.empty())
            });

    final List<Row> data =
        Arrays.asList(
            RowFactory.create(
                "r1",
                Arrays.asList(
                    RowFactory.create("a"), RowFactory.create("b"), RowFactory.create("c"))),
            RowFactory.create("r2", Arrays.asList(RowFactory.create("x"), RowFactory.create("y"))));

    final Dataset<Row> ds = spark.createDataFrame(data, schema).repartition(1);

    // Build a transform that assigns a row index to each array element using the counter
    // expressions.
    final RowIndexCounter counter = new RowIndexCounter();
    final Column indexCol = ValueFunctions.rowCounterGet(counter);

    // Transform each item: struct(value, index), then increment the counter.
    final Column transformed =
        functions.transform(
            col("items"),
            item ->
                ValueFunctions.rowCounterIncrement(
                    struct(item.getField("value").alias("value"), indexCol.alias("idx")), counter));

    // Wrap with resetCounter so the index restarts at zero for each row.
    final Column withReset = ValueFunctions.resetCounter(transformed, counter);

    final Dataset<Row> result = ds.select(col("id"), withReset.alias("indexed_items"));
    final List<Row> rows = result.collectAsList();

    assertEquals(2, rows.size());

    // Row 1: three items with indices 0, 1, 2.
    final List<Row> items1 = rows.get(0).getList(1);
    assertEquals(3, items1.size());
    assertEquals(0, items1.get(0).getInt(1));
    assertEquals(1, items1.get(1).getInt(1));
    assertEquals(2, items1.get(2).getInt(1));

    // Row 2: two items with indices 0, 1 (counter was reset).
    final List<Row> items2 = rows.get(1).getList(1);
    assertEquals(2, items2.size());
    assertEquals(0, items2.get(0).getInt(1));
    assertEquals(1, items2.get(1).getInt(1));
  }
}

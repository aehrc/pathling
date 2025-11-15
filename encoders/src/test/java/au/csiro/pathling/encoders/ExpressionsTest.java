/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
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

import static au.csiro.pathling.encoders.ValueFunctions.ifArray;
import static au.csiro.pathling.encoders.ValueFunctions.ifArray2;
import static au.csiro.pathling.encoders.ValueFunctions.nullIfMissingField;
import static au.csiro.pathling.encoders.ValueFunctions.unnest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Test for FHIR encoders.
 */
public class ExpressionsTest {


  private static SparkSession spark;

  /**
   * Set up Spark.
   */
  @BeforeAll
  static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .getOrCreate();


  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  public static Column size(final Column arr) {
    return ifArray(arr, functions::size,
        x -> functions.when(arr.isNotNull(), functions.lit(1)).otherwise(functions.lit(0)));
  }

  @Test
  void testIfArray() {
    final Dataset<Row> ds = spark.range(2).toDF();
    final Dataset<Row> resultDs = ds
        .withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
        .withColumn("test_single",
            ifArray(ds.col("id"), x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
        .withColumn("test_array",
            ifArray(functions.col("id_array"), x -> functions.array(functions.lit(20)),
                x -> functions.lit(10)))
        .withColumn("test_array_with_unresolved",
            ifArray(functions.col("id_array"), x -> functions.array(functions.col("id_array")),
                x -> functions.lit(10)))
        .withColumn("test_lit",
            ifArray(functions.lit("a1"), x -> functions.array(functions.lit(20)),
                x -> functions.lit(10)))
        .withColumn("test_array_lit",
            ifArray(functions.array(functions.lit("a1")), x -> functions.array(ds.col("id")),
                x -> ds.col("id")))
        .withColumn("test_array_lit_lambda",
            ifArray(functions.filter(functions.array(functions.lit("a1")), c -> c.equalTo("a1")),
                x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
        .withColumn("test_array_size", size(functions.col("id_array")))
        .withColumn("test_array_where", ifArray(functions.col("id_array"),
            x -> functions.filter(x, c -> size(c).equalTo(functions.col("id"))),
            x -> functions.lit(0)));

    final List<Row> results = resultDs.collectAsList();
    assertEquals(2, results.size());

    final Row firstRow = results.getFirst();
    assertEquals(0L, (Long) firstRow.getAs("id"));
    assertEquals(10, (Integer) firstRow.getAs("test_single"));
    assertNotNull(firstRow.getAs("test_array"));
    assertEquals(10, (Integer) firstRow.getAs("test_lit"));
    assertNotNull(firstRow.getAs("test_array_lit"));
    assertEquals(2, (Integer) firstRow.getAs("test_array_size"));

    final Row secondRow = results.get(1);
    assertEquals(1L, (Long) secondRow.getAs("id"));
    assertEquals(10, (Integer) secondRow.getAs("test_single"));
    assertNotNull(secondRow.getAs("test_array"));
    assertEquals(10, (Integer) secondRow.getAs("test_lit"));
    assertNotNull(secondRow.getAs("test_array_lit"));
    assertEquals(2, (Integer) secondRow.getAs("test_array_size"));
  }

  @Test
  void testIfArray2() {
    final Dataset<Row> ds = spark.range(2).toDF();
    final Dataset<Row> resultDs = ds
        .withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
        .withColumn("id_array_of_arrays",
            functions.array(functions.array(functions.col("id"), functions.lit(22)),
                functions.array(functions.col("id"), functions.lit(33))))
        .withColumn("test_single_array",
            ifArray2(functions.col("id_array"),
                x -> functions.array(functions.lit(100)),
                x -> functions.array(functions.lit(50))))
        .withColumn("test_array_of_arrays",
            ifArray2(functions.col("id_array_of_arrays"),
                x -> functions.array(functions.lit(200)),
                x -> functions.array(functions.lit(50))))
        .withColumn("test_flatten_array_of_arrays",
            ifArray2(functions.col("id_array_of_arrays"),
                functions::flatten,
                x -> x))
        .withColumn("test_with_filter",
            ifArray2(functions.col("id_array_of_arrays"),
                x -> functions.filter(x, arr -> functions.size(arr).gt(1)),
                x -> x));

    final List<Row> results = resultDs.collectAsList();
    assertEquals(2, results.size());

    final Row firstRow = results.getFirst();
    assertEquals(0L, (Long) firstRow.getAs("id"));
    // Single array should trigger else branch
    assertNotNull(firstRow.getAs("test_single_array"));
    final Seq<?> singleArrayResult = firstRow.getAs("test_single_array");
    assertEquals(50, CollectionConverters.asJava(singleArrayResult).getFirst());
    // Array of arrays should trigger array branch
    assertNotNull(firstRow.getAs("test_array_of_arrays"));
    final Seq<?> arrayOfArraysResult = firstRow.getAs("test_array_of_arrays");
    assertEquals(200, CollectionConverters.asJava(arrayOfArraysResult).getFirst());
    assertNotNull(firstRow.getAs("test_flatten_array_of_arrays"));
    assertNotNull(firstRow.getAs("test_with_filter"));

    final Row secondRow = results.get(1);
    assertEquals(1L, (Long) secondRow.getAs("id"));
    // Single array should trigger else branch
    assertNotNull(secondRow.getAs("test_single_array"));
    final Seq<?> singleArrayResult2 = secondRow.getAs("test_single_array");
    assertEquals(50, CollectionConverters.asJava(singleArrayResult2).getFirst());
    // Array of arrays should trigger array branch
    assertNotNull(secondRow.getAs("test_array_of_arrays"));
    final Seq<?> arrayOfArraysResult2 = secondRow.getAs("test_array_of_arrays");
    assertEquals(200, CollectionConverters.asJava(arrayOfArraysResult2).getFirst());
    assertNotNull(secondRow.getAs("test_flatten_array_of_arrays"));
    assertNotNull(secondRow.getAs("test_with_filter"));
  }

  @Test
  void testFlatten() {
    final Dataset<Row> ds = spark.range(2).toDF();
    final Dataset<Row> resultDs = ds
        .withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
        .withColumn("id_array_of_arrays",
            functions.array(functions.array(functions.col("id"), functions.lit(22)),
                functions.array(functions.col("id"), functions.lit(33))))
        .withColumn("test_unnest_single", unnest(ds.col("id")))
        .withColumn("test_unnest_array", unnest(functions.col("id_array")))
        .withColumn("test_unnest_array_of_arrays", unnest(functions.col("id_array_of_arrays")))
        .withColumn("test_unnest_array_of_arrays_if_array",
            ifArray(functions.col("id_array"), x -> unnest(functions.col("id_array_of_arrays")),
                x -> x))
        .withColumn("test_unnest_array_of_arrays_if_id",
            ifArray(functions.col("id"), x -> unnest(functions.col("id_array_of_arrays")), x -> x));

    final List<Row> results = resultDs.collectAsList();
    assertEquals(2, results.size());

    final Row firstRow = results.getFirst();
    assertEquals(0L, (Long) firstRow.getAs("id"));
    assertEquals(0L, (Long) firstRow.getAs("test_unnest_single"));
    assertNotNull(firstRow.getAs("test_unnest_array"));
    assertNotNull(firstRow.getAs("test_unnest_array_of_arrays"));
    assertNotNull(firstRow.getAs("test_unnest_array_of_arrays_if_array"));
    assertEquals(0L, (Long) firstRow.getAs("test_unnest_array_of_arrays_if_id"));

    final Row secondRow = results.get(1);
    assertEquals(1L, (Long) secondRow.getAs("id"));
    assertEquals(1L, (Long) secondRow.getAs("test_unnest_single"));
    assertNotNull(secondRow.getAs("test_unnest_array"));
    assertNotNull(secondRow.getAs("test_unnest_array_of_arrays"));
    assertNotNull(secondRow.getAs("test_unnest_array_of_arrays_if_array"));
    assertEquals(1L, (Long) secondRow.getAs("test_unnest_array_of_arrays_if_id"));
  }

  @Test
  void testArrayCrossProd() {
    final Dataset<Row> ds = spark.range(1).toDF();

    // Create three input array columns
    final Dataset<Row> inputDs = ds
        .withColumn("arrayA", functions.array(
            functions.struct(functions.lit("zzz").alias("str"), functions.lit(true).alias("bool")),
            functions.struct(functions.lit("yyy").alias("str"), functions.lit(false).alias("bool"))
        ))
        .withColumn("arrayB", functions.array(
            functions.struct(functions.lit(13).alias("int")),
            functions.struct(functions.lit(17).alias("int")),
            functions.struct(functions.lit(1).alias("int"))
        ))
        .withColumn("arrayC", functions.array(
            functions.struct(functions.lit(13.1).alias("float")),
            functions.struct(functions.lit(17.2).alias("float"))
        ));

    // Test structProduct
    final Dataset<Row> structProductDs = inputDs
        .withColumn("sp_one", ColumnFunctions.structProduct(inputDs.col("arrayA")))
        .withColumn("sp_two",
            ColumnFunctions.structProduct(inputDs.col("arrayA"), inputDs.col("arrayB")))
        .withColumn("sp_three",
            ColumnFunctions.structProduct(inputDs.col("arrayA"), inputDs.col("arrayB"),
                inputDs.col("arrayC")));

    // Test structProductOuter
    final Dataset<Row> structProductOuterDs = inputDs
        .withColumn("spo_one", ColumnFunctions.structProductOuter(inputDs.col("arrayA")))
        .withColumn("spo_two",
            ColumnFunctions.structProductOuter(inputDs.col("arrayA"), inputDs.col("arrayB")))
        .withColumn("spo_three",
            ColumnFunctions.structProductOuter(inputDs.col("arrayA"), inputDs.col("arrayB"),
                inputDs.col("arrayC")));

    // Collect and assert structProduct results
    final List<Row> spResults = structProductDs.collectAsList();
    assertEquals(1, spResults.size());
    final Row spRow = spResults.getFirst();
    assertNotNull(spRow.getAs("sp_one"));
    assertNotNull(spRow.getAs("sp_two"));
    assertNotNull(spRow.getAs("sp_three"));

    // Collect and assert structProductOuter results
    final List<Row> spoResults = structProductOuterDs.collectAsList();
    assertEquals(1, spoResults.size());
    final Row spoRow = spoResults.getFirst();
    assertNotNull(spoRow.getAs("spo_one"));
    assertNotNull(spoRow.getAs("spo_two"));
    assertNotNull(spoRow.getAs("spo_three"));

    // Assert sizes against expected product
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

    // Create input array columns
    final Dataset<Row> inputDs = ds
        .withColumn("nullArray",
            functions.lit(null).cast(DataTypes.createArrayType(DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("str", DataTypes.StringType, true),
                    DataTypes.createStructField("int", DataTypes.IntegerType, true)
                )
            ))))
        .withColumn("emptyArray",
            functions.array().cast(DataTypes.createArrayType(DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("str", DataTypes.StringType, true),
                    DataTypes.createStructField("int", DataTypes.IntegerType, true)
                )
            ))))
        .withColumn("nonEmptyArray", functions.array(
            functions.struct(functions.lit("zzz").alias("str"), functions.lit(1).alias("int")),
            functions.struct(functions.lit("yyy").alias("str"), functions.lit(2).alias("int"))
        ));

    // Test structProduct
    final Dataset<Row> structProductDs = inputDs
        .withColumn("sp_null", ColumnFunctions.structProduct(inputDs.col("nullArray")))
        .withColumn("sp_empty", ColumnFunctions.structProduct(inputDs.col("emptyArray")))
        .withColumn("sp_nonEmpty",
            ColumnFunctions.structProduct(inputDs.col("nonEmptyArray"), inputDs.col("emptyArray")));

    // Test structProductOuter
    final Dataset<Row> structProductOuterDs = inputDs
        .withColumn("spo_null", ColumnFunctions.structProductOuter(inputDs.col("nullArray")))
        .withColumn("spo_empty", ColumnFunctions.structProductOuter(inputDs.col("emptyArray")))
        .withColumn("spo_nonEmpty", ColumnFunctions.structProductOuter(inputDs.col("nonEmptyArray"),
            inputDs.col("emptyArray")));

    // Collect and assert structProduct results
    final List<Row> spResults = structProductDs.collectAsList();
    assertEquals(1, spResults.size());
    final Row spRow = spResults.getFirst();
    assertNotNull(spRow.getAs("sp_null"));
    assertEquals(0, ((Seq<?>) spRow.getAs("sp_null")).size());
    assertNotNull(spRow.getAs("sp_empty"));
    assertEquals(0, ((Seq<?>) spRow.getAs("sp_empty")).size());
    assertNotNull(spRow.getAs("sp_nonEmpty"));
    assertEquals(0, ((Seq<?>) spRow.getAs("sp_nonEmpty")).size());

    // Collect and assert structProductOuter results
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

  @Test
  void testPruneAnnotations() {
    final Metadata metadata = Metadata.empty();
    final StructType valueStructType = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, true, metadata),
        new StructField("name", DataTypes.StringType, true, metadata),
        new StructField("_fid", DataTypes.StringType, true, metadata)
    });

    final StructType inputSchema = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.StringType, true, metadata),
        new StructField("active", DataTypes.BooleanType, true, metadata),
        new StructField("gender", DataTypes.createArrayType(DataTypes.StringType), true, metadata),
        new StructField("value", valueStructType, true, metadata)
    });

    final List<Row> inputData = Arrays.asList(
        RowFactory.create("patient-1", true, new String[]{"array_value-00-00"},
            RowFactory.create(1, "Test-1", "fid_value-00")),
        RowFactory.create("patient-2", false,
            new String[]{"array_value-01-00", "array_value-01-01"},
            RowFactory.create(2, "Test-2", "fid_value_01")),
        RowFactory.create("patient-3", null, null, null)
    );

    final Dataset<Row> dataset = spark.createDataFrame(inputData, inputSchema).repartition(1);

    // Prune all columns
    final Dataset<Row> prunedDataset = dataset.select(
        Stream.of(dataset.columns())
            .map(cn -> ValueFunctions.pruneAnnotations(dataset.col(cn)).alias(cn))
            .toArray(Column[]::new));

    // Expected result has the _fid field removed from the value struct
    final StructType expectedValueStructType = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, true, metadata),
        new StructField("name", DataTypes.StringType, true, metadata)
    });

    final StructType expectedSchema = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.StringType, true, metadata),
        new StructField("active", DataTypes.BooleanType, true, metadata),
        new StructField("gender", DataTypes.createArrayType(DataTypes.StringType), true, metadata),
        new StructField("value", expectedValueStructType, true, metadata)
    });

    final List<Row> expectedData = Arrays.asList(
        RowFactory.create("patient-1", true, new String[]{"array_value-00-00"},
            RowFactory.create(1, "Test-1")),
        RowFactory.create("patient-2", false,
            new String[]{"array_value-01-00", "array_value-01-01"},
            RowFactory.create(2, "Test-2")),
        RowFactory.create("patient-3", null, null, null)
    );

    final Dataset<Row> expectedResult = spark.createDataFrame(expectedData, expectedSchema)
        .repartition(1);

    assertEquals(expectedResult.schema(), prunedDataset.schema());
    assertEquals(expectedResult.collectAsList(), prunedDataset.collectAsList());
  }

  @Test
  void testPruneAnnotationsWithGroupBy() {
    final Metadata metadata = Metadata.empty();
    final StructType valueStructType = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, true, metadata),
        new StructField("name", DataTypes.StringType, true, metadata),
        new StructField("_fid", DataTypes.StringType, true, metadata)
    });

    final StructType inputSchema = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.StringType, true, metadata),
        new StructField("gender", DataTypes.StringType, true, metadata),
        new StructField("active", DataTypes.BooleanType, true, metadata),
        new StructField("value", valueStructType, true, metadata)
    });

    final List<Row> inputData = Arrays.asList(
        RowFactory.create("patient-1", "male", true, RowFactory.create(1, "Test-1", "fid-00")),
        RowFactory.create("patient-2", "female", false, RowFactory.create(2, "Test-2", "fid-01")),
        RowFactory.create("patient-3", "male", true, null),
        RowFactory.create("patient-4", null, true, null),
        RowFactory.create("patient-5", "female", false, RowFactory.create(2, "Test-2", "fid-02"))
    );

    final Dataset<Row> dataset = spark.createDataFrame(inputData, inputSchema).repartition(1);

    // Group by pruned value column and aggregate
    final Dataset<Row> grouped = dataset.groupBy(
            ValueFunctions.pruneAnnotations(dataset.col("value")))
        .agg(functions.count(dataset.col("gender")).alias("count"));

    // Select with proper column names
    final Dataset<Row> groupedResult = grouped
        .select(
            functions.col(grouped.columns()[0]).alias("value"),
            functions.col("count")).orderBy(functions.col("value.id").asc_nulls_first());

    final StructType expectedValueStructType = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, true, metadata),
        new StructField("name", DataTypes.StringType, true, metadata)
    });

    final StructType expectedSchema = DataTypes.createStructType(new StructField[]{
        new StructField("value", expectedValueStructType, true, metadata),
        new StructField("count", DataTypes.LongType, false, metadata)
    });

    final List<Row> expectedData = Arrays.asList(
        RowFactory.create(null, 1L),
        RowFactory.create(RowFactory.create(1, "Test-1"), 1L),
        RowFactory.create(RowFactory.create(2, "Test-2"), 2L)
    );

    final Dataset<Row> expectedResult = spark.createDataFrame(expectedData, expectedSchema);

    assertEquals(expectedResult.schema(), groupedResult.schema());
    assertEquals(expectedResult.collectAsList(), groupedResult.collectAsList());
  }

  /**
   * Helper method to create a nested item structure with 3 levels for testing transformTree.
   * The structure has different types at each level to enable testing of type-based traversal limits.
   *
   * @return Dataset with structure: items[level0Type(item[level1Type(item[level2Type])])]
   */
  private Dataset<Row> createNestedItemDataset() {
    final Metadata metadata = Metadata.empty();

    // Level 2 (leaf): NO item field - different type from level1
    final StructType level2Type = DataTypes.createStructType(new StructField[]{
        new StructField("linkId", DataTypes.StringType, true, metadata),
        new StructField("text", DataTypes.StringType, true, metadata)
    });

    // Level 1: HAS item field (array of level2Type) - different type from level0
    final StructType level1Type = DataTypes.createStructType(new StructField[]{
        new StructField("linkId", DataTypes.StringType, true, metadata),
        new StructField("text", DataTypes.StringType, true, metadata),
        new StructField("item", DataTypes.createArrayType(level2Type), true, metadata)
    });

    // Level 0: HAS item field (array of level1Type) - root level type
    final StructType level0Type = DataTypes.createStructType(new StructField[]{
        new StructField("linkId", DataTypes.StringType, true, metadata),
        new StructField("text", DataTypes.StringType, true, metadata),
        new StructField("item", DataTypes.createArrayType(level1Type), true, metadata)
    });

    // Create test data: 3 levels deep
    final Row level2Item = RowFactory.create("3", "Level 2");
    final Row level1Item = RowFactory.create("2", "Level 1", List.of(level2Item));
    final Row level0Item = RowFactory.create("1", "Level 0", List.of(level1Item));

    final StructType rootSchema = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, true, metadata),
        new StructField("items", DataTypes.createArrayType(level0Type), true, metadata)
    });

    return spark.createDataFrame(
        List.of(RowFactory.create(1, List.of(level0Item))),
        rootSchema
    );
  }

  @Test
  void testTransformTreeFinitePathWithDifferentTypes() {
    // Finite path: traversing via getField('item') through structurally different types
    // Each level has a different schema type (level0Type != level1Type != level2Type)
    // maxDepth should NOT apply because types change at each traversal step

    final Dataset<Row> ds = createNestedItemDataset();

    // Traverse with getField('item') - each traversal step changes type
    // Level 0 (level0Type) -> getField('item') -> Level 1 (level1Type) -> getField('item') -> Level 2 (level2Type)
    // maxDepth=1 is very strict, but should NOT limit because types change
    final Dataset<Row> result = ds.withColumn("collected",
        ValueFunctions.transformTree(
            ds.col("items"),
            c -> c.getField("linkId"),  // Extract linkId at each level
            List.of(c -> unnest(c.getField("item"))),  // Traverse via item field (type changes)
            1  // maxDepth=1 (strict limit)
        )
    );

    final List<Row> results = result.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.getFirst();
    final Seq<?> collected = row.getAs("collected");
    final List<?> linkIds = CollectionConverters.asJava(collected);

    // Should collect ALL 3 levels because each level has different type
    // "1" (level 0), "2" (level 1), "3" (level 2)
    assertEquals(List.of("1", "2", "3"), linkIds);
  }

  @Test
  void testTransformTreeSelfReferentialInfiniteLoop() {
    // Self-referential: traversing with identity function c -> c
    // This would create infinite recursion (same type forever)
    // maxDepth MUST apply to prevent infinite loop

    final Dataset<Row> ds = createNestedItemDataset();

    // Traverse with identity function c -> c
    // This would loop infinitely: items -> items -> items -> items ...
    // Type never changes (always array<level0Type>), so maxDepth applies
    // maxDepth=1 should limit to only 2 iterations (levels 0 and 1)
    final Dataset<Row> result = ds.withColumn("collected",
        ValueFunctions.transformTree(
            ds.col("items"),
            c -> c.getField("linkId"),  // Extract linkId at each level
            List.of(c -> c),  // Identity function: infinite loop!
            1  // maxDepth=1 must prevent infinite recursion
        )
    );

    final List<Row> results = result.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.getFirst();
    final Seq<?> collected = row.getAs("collected");
    final List<?> linkIds = CollectionConverters.asJava(collected);

    // Should collect only 2 levels (0 and 1) because:
    // - Level 0: items (type = array<level0Type>)
    // - Level 1: c -> c returns items again (type = array<level0Type>, same type!)
    // - Level 2: would be items again but maxDepth=1 prevents it
    // All extracted linkIds should be "1" (same item repeated)
    assertEquals(List.of("1", "1"), linkIds);
  }

  @Test
  void testTransformTreeMultipleTraversalPaths() {
    // Multiple traversal paths: combines finite path (type-changing) and self-referential (infinite loop)
    // This demonstrates that different paths can have different depth behaviors simultaneously

    final Dataset<Row> ds = createNestedItemDataset();

    // Use TWO traversal paths:
    // Path 1: c -> unnest(c.getField("item")) - finite path with type changes
    // Path 2: c -> c - self-referential with same type (infinite loop)
    final Dataset<Row> result = ds.withColumn("collected",
        ValueFunctions.transformTree(
            ds.col("items"),
            c -> c.getField("linkId"),  // Extract linkId at each level
            List.of(
                c -> unnest(c.getField("item")),  // Path 1: finite, type changes
                c -> c                             // Path 2: infinite loop, type stays same
            ),
            1  // maxDepth=1
        )
    );

    final List<Row> results = result.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.getFirst();
    final Seq<?> collected = row.getAs("collected");
    final List<?> linkIds = CollectionConverters.asJava(collected);

    // Expected results from both paths:
    // Path 1 (finite, type-changing): traverses through item field
    //   - Extracts: "1" (root), "2" (level 1), "3" (level 2)
    // Path 2 (self-referential, c->c): extracts from current node at each traversal level
    //   - At level 2 (node with linkId=3): extract "3"
    //   - At level 1 (node with linkId=2): extract "2"
    //   - At level 0 (node with linkId=1): extract "1"
    // Total result: [1, 2, 3] from Path 1, then [3, 2, 1] from Path 2 going back up
    assertEquals(List.of("1", "2", "3", "3", "2", "1"), linkIds);
  }

  @Test
  void testNullIfMissingField() {
    // Comprehensive test for nullIfMissingField covering all scenarios:
    // 1. Existing top-level fields returning actual values
    // 2. Existing nested struct fields returning actual values
    // 3. Missing struct fields returning null
    // 4. Struct fields with null values vs non-existent fields

    final Metadata metadata = Metadata.empty();

    // Create a struct type for person with name and age fields (no email or salary fields)
    final StructType personType = DataTypes.createStructType(new StructField[]{
        new StructField("name", DataTypes.StringType, true, metadata),
        new StructField("age", DataTypes.IntegerType, true, metadata)
    });

    final StructType schema = DataTypes.createStructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, true, metadata),
        new StructField("value", DataTypes.IntegerType, true, metadata),
        new StructField("person", personType, true, metadata)
    });

    final List<Row> data = List.of(
        RowFactory.create(1, 100, RowFactory.create(null, 25)),  // person.name is null
        RowFactory.create(2, 200, RowFactory.create("Bob", null)),  // person.age is null
        RowFactory.create(3, 300, RowFactory.create("Charlie", 30))
    );

    final Dataset<Row> ds = spark.createDataFrame(data, schema);

    // Apply multiple nullIfMissingField expressions to test all scenarios
    final Dataset<Row> result = ds
        // Test 1: Existing top-level field
        .withColumn("test_top_level", nullIfMissingField(ds.col("value")))
        // Test 2: Existing nested field using dot notation
        .withColumn("test_nested_exists", nullIfMissingField(ds.col("person.name")))
        // Test 3: Existing nested field using getField
        .withColumn("test_struct_field", nullIfMissingField(ds.col("person").getField("age")))
        // Test 4: Missing struct field (address doesn't exist)
        .withColumn("test_missing_address", nullIfMissingField(ds.col("person").getField("address")))
        // Test 5: Missing struct field (email doesn't exist)
        .withColumn("test_missing_email", nullIfMissingField(ds.col("person").getField("email")))
        // Test 6: Missing struct field (salary doesn't exist)
        .withColumn("test_missing_salary", nullIfMissingField(ds.col("person").getField("salary")));

    final List<Row> results = result.collectAsList();
    assertEquals(3, results.size());

    // Row 1: person.name is null, person.age is 25
    assertEquals(100, (Integer) results.getFirst().getAs("test_top_level"));
    assertNull(results.getFirst().getAs("test_nested_exists"));  // null value from data
    assertEquals(25, (Integer) results.getFirst().getAs("test_struct_field"));
    assertNull(results.getFirst().getAs("test_missing_address"));  // field doesn't exist
    assertNull(results.get(0).getAs("test_missing_email"));
    assertNull(results.get(0).getAs("test_missing_salary"));

    // Row 2: person.name is "Bob", person.age is null
    assertEquals(200, (Integer) results.get(1).getAs("test_top_level"));
    assertEquals("Bob", results.get(1).getAs("test_nested_exists"));
    assertNull(results.get(1).getAs("test_struct_field"));  // null value from data
    assertNull(results.get(1).getAs("test_missing_address"));  // field doesn't exist
    assertNull(results.get(1).getAs("test_missing_email"));
    assertNull(results.get(1).getAs("test_missing_salary"));

    // Row 3: person.name is "Charlie", person.age is 30
    assertEquals(300, (Integer) results.get(2).getAs("test_top_level"));
    assertEquals("Charlie", results.get(2).getAs("test_nested_exists"));
    assertEquals(30, (Integer) results.get(2).getAs("test_struct_field"));
    assertNull(results.get(2).getAs("test_missing_address"));  // field doesn't exist
    assertNull(results.get(2).getAs("test_missing_email"));
    assertNull(results.get(2).getAs("test_missing_salary"));

    // Key distinction: Fields that exist but have null values (row 1 name, row 2 age)
    // preserve the null from the data, while missing fields (address, email, salary)
    // return null because they don't exist in the struct schema
  }
}

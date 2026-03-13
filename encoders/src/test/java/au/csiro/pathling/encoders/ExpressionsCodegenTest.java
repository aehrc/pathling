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

/** Tests for FHIR encoder expressions with whole-stage codegen enabled (default). */
public class ExpressionsCodegenTest extends ExpressionsBothModesTest {

  /** Set up Spark with codegen enabled (the default). */
  @BeforeAll
  static void setUpCodegen() {
    setUp();
  }

  /** Tear down Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  public static Column size(final Column arr) {
    return ifArray(
        arr,
        functions::size,
        x -> functions.when(arr.isNotNull(), functions.lit(1)).otherwise(functions.lit(0)));
  }

  @Test
  void testIfArray() {
    final Dataset<Row> ds = spark.range(2).toDF();
    final Dataset<Row> resultDs =
        ds.withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
            .withColumn(
                "test_single",
                ifArray(
                    ds.col("id"), x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
            .withColumn(
                "test_array",
                ifArray(
                    functions.col("id_array"),
                    x -> functions.array(functions.lit(20)),
                    x -> functions.lit(10)))
            .withColumn(
                "test_array_with_unresolved",
                ifArray(
                    functions.col("id_array"),
                    x -> functions.array(functions.col("id_array")),
                    x -> functions.lit(10)))
            .withColumn(
                "test_lit",
                ifArray(
                    functions.lit("a1"),
                    x -> functions.array(functions.lit(20)),
                    x -> functions.lit(10)))
            .withColumn(
                "test_array_lit",
                ifArray(
                    functions.array(functions.lit("a1")),
                    x -> functions.array(ds.col("id")),
                    x -> ds.col("id")))
            .withColumn(
                "test_array_lit_lambda",
                ifArray(
                    functions.filter(functions.array(functions.lit("a1")), c -> c.equalTo("a1")),
                    x -> functions.array(functions.lit(20)),
                    x -> functions.lit(10)))
            .withColumn("test_array_size", size(functions.col("id_array")))
            .withColumn(
                "test_array_where",
                ifArray(
                    functions.col("id_array"),
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
    final Dataset<Row> resultDs =
        ds.withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
            .withColumn(
                "id_array_of_arrays",
                functions.array(
                    functions.array(functions.col("id"), functions.lit(22)),
                    functions.array(functions.col("id"), functions.lit(33))))
            .withColumn(
                "test_single_array",
                ifArray2(
                    functions.col("id_array"),
                    x -> functions.array(functions.lit(100)),
                    x -> functions.array(functions.lit(50))))
            .withColumn(
                "test_array_of_arrays",
                ifArray2(
                    functions.col("id_array_of_arrays"),
                    x -> functions.array(functions.lit(200)),
                    x -> functions.array(functions.lit(50))))
            .withColumn(
                "test_flatten_array_of_arrays",
                ifArray2(functions.col("id_array_of_arrays"), functions::flatten, x -> x))
            .withColumn(
                "test_with_filter",
                ifArray2(
                    functions.col("id_array_of_arrays"),
                    x -> functions.filter(x, arr -> functions.size(arr).gt(1)),
                    x -> x));

    final List<Row> results = resultDs.collectAsList();
    assertEquals(2, results.size());

    final Row firstRow = results.getFirst();
    assertEquals(0L, (Long) firstRow.getAs("id"));
    // Single array should trigger else branch.
    assertNotNull(firstRow.getAs("test_single_array"));
    final Seq<?> singleArrayResult = firstRow.getAs("test_single_array");
    assertEquals(50, CollectionConverters.asJava(singleArrayResult).getFirst());
    // Array of arrays should trigger array branch.
    assertNotNull(firstRow.getAs("test_array_of_arrays"));
    final Seq<?> arrayOfArraysResult = firstRow.getAs("test_array_of_arrays");
    assertEquals(200, CollectionConverters.asJava(arrayOfArraysResult).getFirst());
    assertNotNull(firstRow.getAs("test_flatten_array_of_arrays"));
    assertNotNull(firstRow.getAs("test_with_filter"));

    final Row secondRow = results.get(1);
    assertEquals(1L, (Long) secondRow.getAs("id"));
    // Single array should trigger else branch.
    assertNotNull(secondRow.getAs("test_single_array"));
    final Seq<?> singleArrayResult2 = secondRow.getAs("test_single_array");
    assertEquals(50, CollectionConverters.asJava(singleArrayResult2).getFirst());
    // Array of arrays should trigger array branch.
    assertNotNull(secondRow.getAs("test_array_of_arrays"));
    final Seq<?> arrayOfArraysResult2 = secondRow.getAs("test_array_of_arrays");
    assertEquals(200, CollectionConverters.asJava(arrayOfArraysResult2).getFirst());
    assertNotNull(secondRow.getAs("test_flatten_array_of_arrays"));
    assertNotNull(secondRow.getAs("test_with_filter"));
  }

  @Test
  void testFlatten() {
    final Dataset<Row> ds = spark.range(2).toDF();
    final Dataset<Row> resultDs =
        ds.withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
            .withColumn(
                "id_array_of_arrays",
                functions.array(
                    functions.array(functions.col("id"), functions.lit(22)),
                    functions.array(functions.col("id"), functions.lit(33))))
            .withColumn("test_unnest_single", unnest(ds.col("id")))
            .withColumn("test_unnest_array", unnest(functions.col("id_array")))
            .withColumn("test_unnest_array_of_arrays", unnest(functions.col("id_array_of_arrays")))
            .withColumn(
                "test_unnest_array_of_arrays_if_array",
                ifArray(
                    functions.col("id_array"),
                    x -> unnest(functions.col("id_array_of_arrays")),
                    x -> x))
            .withColumn(
                "test_unnest_array_of_arrays_if_id",
                ifArray(
                    functions.col("id"), x -> unnest(functions.col("id_array_of_arrays")), x -> x));

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
  void testPruneAnnotations() {
    final Metadata metadata = Metadata.empty();
    final StructType valueStructType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, metadata),
              new StructField("name", DataTypes.StringType, true, metadata),
              new StructField("_fid", DataTypes.StringType, true, metadata)
            });

    final StructType inputSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, metadata),
              new StructField("active", DataTypes.BooleanType, true, metadata),
              new StructField(
                  "gender", DataTypes.createArrayType(DataTypes.StringType), true, metadata),
              new StructField("value", valueStructType, true, metadata)
            });

    final List<Row> inputData =
        Arrays.asList(
            RowFactory.create(
                "patient-1",
                true,
                new String[] {"array_value-00-00"},
                RowFactory.create(1, "Test-1", "fid_value-00")),
            RowFactory.create(
                "patient-2",
                false,
                new String[] {"array_value-01-00", "array_value-01-01"},
                RowFactory.create(2, "Test-2", "fid_value_01")),
            RowFactory.create("patient-3", null, null, null));

    final Dataset<Row> dataset = spark.createDataFrame(inputData, inputSchema).repartition(1);

    // Prune all columns.
    final Dataset<Row> prunedDataset =
        dataset.select(
            Stream.of(dataset.columns())
                .map(cn -> ValueFunctions.pruneAnnotations(dataset.col(cn)).alias(cn))
                .toArray(Column[]::new));

    // Expected result has the _fid field removed from the value struct.
    final StructType expectedValueStructType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, metadata),
              new StructField("name", DataTypes.StringType, true, metadata)
            });

    final StructType expectedSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, metadata),
              new StructField("active", DataTypes.BooleanType, true, metadata),
              new StructField(
                  "gender", DataTypes.createArrayType(DataTypes.StringType), true, metadata),
              new StructField("value", expectedValueStructType, true, metadata)
            });

    final List<Row> expectedData =
        Arrays.asList(
            RowFactory.create(
                "patient-1",
                true,
                new String[] {"array_value-00-00"},
                RowFactory.create(1, "Test-1")),
            RowFactory.create(
                "patient-2",
                false,
                new String[] {"array_value-01-00", "array_value-01-01"},
                RowFactory.create(2, "Test-2")),
            RowFactory.create("patient-3", null, null, null));

    final Dataset<Row> expectedResult =
        spark.createDataFrame(expectedData, expectedSchema).repartition(1);

    assertEquals(expectedResult.schema(), prunedDataset.schema());
    assertEquals(expectedResult.collectAsList(), prunedDataset.collectAsList());
  }

  @Test
  void testPruneAnnotationsWithGroupBy() {
    final Metadata metadata = Metadata.empty();
    final StructType valueStructType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, metadata),
              new StructField("name", DataTypes.StringType, true, metadata),
              new StructField("_fid", DataTypes.StringType, true, metadata)
            });

    final StructType inputSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, true, metadata),
              new StructField("gender", DataTypes.StringType, true, metadata),
              new StructField("active", DataTypes.BooleanType, true, metadata),
              new StructField("value", valueStructType, true, metadata)
            });

    final List<Row> inputData =
        Arrays.asList(
            RowFactory.create("patient-1", "male", true, RowFactory.create(1, "Test-1", "fid-00")),
            RowFactory.create(
                "patient-2", "female", false, RowFactory.create(2, "Test-2", "fid-01")),
            RowFactory.create("patient-3", "male", true, null),
            RowFactory.create("patient-4", null, true, null),
            RowFactory.create(
                "patient-5", "female", false, RowFactory.create(2, "Test-2", "fid-02")));

    final Dataset<Row> dataset = spark.createDataFrame(inputData, inputSchema).repartition(1);

    // Group by pruned value column and aggregate.
    final Dataset<Row> grouped =
        dataset
            .groupBy(ValueFunctions.pruneAnnotations(dataset.col("value")))
            .agg(functions.count(dataset.col("gender")).alias("count"));

    // Select with proper column names.
    final Dataset<Row> groupedResult =
        grouped
            .select(functions.col(grouped.columns()[0]).alias("value"), functions.col("count"))
            .orderBy(functions.col("value.id").asc_nulls_first());

    final StructType expectedValueStructType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, metadata),
              new StructField("name", DataTypes.StringType, true, metadata)
            });

    final StructType expectedSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("value", expectedValueStructType, true, metadata),
              new StructField("count", DataTypes.LongType, false, metadata)
            });

    final List<Row> expectedData =
        Arrays.asList(
            RowFactory.create(null, 1L),
            RowFactory.create(RowFactory.create(1, "Test-1"), 1L),
            RowFactory.create(RowFactory.create(2, "Test-2"), 2L));

    final Dataset<Row> expectedResult = spark.createDataFrame(expectedData, expectedSchema);

    assertEquals(expectedResult.schema(), groupedResult.schema());
    assertEquals(expectedResult.collectAsList(), groupedResult.collectAsList());
  }

  /**
   * Helper method to create a nested item structure with 3 levels for testing transformTree. The
   * structure has different types at each level to enable testing of type-based traversal limits.
   *
   * @return Dataset with structure: items[level0Type(item[level1Type(item[level2Type])])]
   */
  private Dataset<Row> createNestedItemDataset() {
    final Metadata metadata = Metadata.empty();

    // Level 2 (leaf): NO item field - different type from level1.
    final StructType level2Type =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("text", DataTypes.StringType, true, metadata)
            });

    // Level 1: HAS item field (array of level2Type) - different type from level0.
    final StructType level1Type =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("text", DataTypes.StringType, true, metadata),
              new StructField("item", DataTypes.createArrayType(level2Type), true, metadata)
            });

    // Level 0: HAS item field (array of level1Type) - root level type.
    final StructType level0Type =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("linkId", DataTypes.StringType, true, metadata),
              new StructField("text", DataTypes.StringType, true, metadata),
              new StructField("item", DataTypes.createArrayType(level1Type), true, metadata)
            });

    // Create test data: 3 levels deep.
    final Row level2Item = RowFactory.create("3", "Level 2");
    final Row level1Item = RowFactory.create("2", "Level 1", List.of(level2Item));
    final Row level0Item = RowFactory.create("1", "Level 0", List.of(level1Item));

    final StructType rootSchema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, metadata),
              new StructField("items", DataTypes.createArrayType(level0Type), true, metadata)
            });

    return spark.createDataFrame(List.of(RowFactory.create(1, List.of(level0Item))), rootSchema);
  }

  @Test
  void testTransformTreeFinitePathWithDifferentTypes() {
    final Dataset<Row> ds = createNestedItemDataset();

    final Dataset<Row> result =
        ds.withColumn(
            "collected",
            ValueFunctions.transformTree(
                ds.col("items"),
                c -> c.getField("linkId"),
                List.of(c -> unnest(c.getField("item"))),
                1));

    final List<Row> results = result.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.getFirst();
    final Seq<?> collected = row.getAs("collected");
    final List<?> linkIds = CollectionConverters.asJava(collected);

    assertEquals(List.of("1", "2", "3"), linkIds);
  }

  @Test
  void testTransformTreeSelfReferentialInfiniteLoop() {
    final Dataset<Row> ds = createNestedItemDataset();

    final Dataset<Row> result =
        ds.withColumn(
            "collected",
            ValueFunctions.transformTree(
                ds.col("items"), c -> c.getField("linkId"), List.of(c -> c), 2));

    final List<Row> results = result.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.getFirst();
    final Seq<?> collected = row.getAs("collected");
    final List<?> linkIds = CollectionConverters.asJava(collected);

    assertEquals(List.of("1", "1", "1"), linkIds);
  }

  @Test
  void testTransformTreeMultipleTraversalPaths() {
    final Dataset<Row> ds = createNestedItemDataset();

    final Dataset<Row> result =
        ds.withColumn(
            "collected",
            ValueFunctions.transformTree(
                ds.col("items"),
                c -> c.getField("linkId"),
                List.of(c -> unnest(c.getField("item")), c -> c),
                1));

    final List<Row> results = result.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.getFirst();
    final Seq<?> collected = row.getAs("collected");
    final List<?> linkIds = CollectionConverters.asJava(collected);

    assertEquals(List.of("1", "2", "3", "3", "2", "3", "1", "2", "3"), linkIds);
  }

  @Test
  void testNullIfMissingField() {
    final Metadata metadata = Metadata.empty();

    final StructType personType =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("name", DataTypes.StringType, true, metadata),
              new StructField("age", DataTypes.IntegerType, true, metadata)
            });

    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, metadata),
              new StructField("value", DataTypes.IntegerType, true, metadata),
              new StructField("person", personType, true, metadata)
            });

    final List<Row> data =
        List.of(
            RowFactory.create(1, 100, RowFactory.create(null, 25)),
            RowFactory.create(2, 200, RowFactory.create("Bob", null)),
            RowFactory.create(3, 300, RowFactory.create("Charlie", 30)));

    final Dataset<Row> ds = spark.createDataFrame(data, schema);

    final Dataset<Row> result =
        ds.withColumn("test_top_level", nullIfMissingField(ds.col("value")))
            .withColumn("test_nested_exists", nullIfMissingField(ds.col("person.name")))
            .withColumn("test_struct_field", nullIfMissingField(ds.col("person").getField("age")))
            .withColumn(
                "test_missing_address", nullIfMissingField(ds.col("person").getField("address")))
            .withColumn(
                "test_missing_email", nullIfMissingField(ds.col("person").getField("email")))
            .withColumn(
                "test_missing_salary", nullIfMissingField(ds.col("person").getField("salary")));

    final List<Row> results = result.collectAsList();
    assertEquals(3, results.size());

    // Row 1: person.name is null, person.age is 25.
    assertEquals(100, (Integer) results.getFirst().getAs("test_top_level"));
    assertNull(results.getFirst().getAs("test_nested_exists"));
    assertEquals(25, (Integer) results.getFirst().getAs("test_struct_field"));
    assertNull(results.getFirst().getAs("test_missing_address"));
    assertNull(results.get(0).getAs("test_missing_email"));
    assertNull(results.get(0).getAs("test_missing_salary"));

    // Row 2: person.name is "Bob", person.age is null.
    assertEquals(200, (Integer) results.get(1).getAs("test_top_level"));
    assertEquals("Bob", results.get(1).getAs("test_nested_exists"));
    assertNull(results.get(1).getAs("test_struct_field"));
    assertNull(results.get(1).getAs("test_missing_address"));
    assertNull(results.get(1).getAs("test_missing_email"));
    assertNull(results.get(1).getAs("test_missing_salary"));

    // Row 3: person.name is "Charlie", person.age is 30.
    assertEquals(300, (Integer) results.get(2).getAs("test_top_level"));
    assertEquals("Charlie", results.get(2).getAs("test_nested_exists"));
    assertEquals(30, (Integer) results.get(2).getAs("test_struct_field"));
    assertNull(results.get(2).getAs("test_missing_address"));
    assertNull(results.get(2).getAs("test_missing_email"));
    assertNull(results.get(2).getAs("test_missing_salary"));
  }
}

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
 */

package au.csiro.pathling.encoders;

import static au.csiro.pathling.encoders.ValueFunctions.ifArray;
import static au.csiro.pathling.encoders.ValueFunctions.unnest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    final Row firstRow = results.get(0);
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

    final Row firstRow = results.get(0);
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
    final Dataset<Row> resultDs = ds
        .withColumn("one_array", ColumnFunctions.structProduct(
                functions.array(functions.struct(
                    functions.lit("xxx").alias("str"),
                    functions.lit(10).alias("int")
                ))
            )
        )
        .withColumn("two_arrays", ColumnFunctions.structProduct(
                functions.array(
                    functions.struct(
                        functions.lit("zzz").alias("str")
                    ),
                    functions.struct(
                        functions.lit("yyy").alias("str")
                    )
                ),
                functions.array(
                    functions.struct(
                        functions.lit(13).alias("int")
                    ),
                    functions.struct(
                        functions.lit(17).alias("int")
                    ),
                    functions.struct(
                        functions.lit(1).alias("int")
                    )
                )
            )
        )
        .withColumn("three_arrays", ColumnFunctions.structProduct(
                functions.array(
                    functions.struct(
                        functions.lit("zzz").alias("str"),
                        functions.lit(true).alias("bool")
                    ),
                    functions.struct(
                        functions.lit("yyy").alias("str"),
                        functions.lit(false).alias("bool")
                    )
                ),
                functions.array(
                    functions.struct(
                        functions.lit(13).alias("int")
                    ),
                    functions.struct(
                        functions.lit(17).alias("int")
                    ),
                    functions.struct(
                        functions.lit(1).alias("int")
                    )
                ),
                functions.array(
                    functions.struct(
                        functions.lit(13.1).alias("float")
                    ),
                    functions.struct(
                        functions.lit(17.2).alias("float")
                    )
                )
            )
        );

    final List<Row> results = resultDs.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.get(0);
    assertEquals(0L, (Long) row.getAs("id"));
    assertNotNull(row.getAs("one_array"));
    assertNotNull(row.getAs("two_arrays"));
    assertNotNull(row.getAs("three_arrays"));
  }


  @Test
  void testArrayCrossProdWithNullsAndEmptys() {
    final Dataset<Row> ds = spark.range(1).toDF();
    final Dataset<Row> resultDs = ds
        .withColumn("null", ColumnFunctions.structProduct(
                functions.lit(null).cast(DataTypes.createArrayType(DataTypes.createStructType(
                    Arrays.asList(
                        DataTypes.createStructField("str", DataTypes.StringType, true),
                        DataTypes.createStructField("int", DataTypes.IntegerType, true)
                    )
                )))
            )
        )
        .withColumn("emptyArray", ColumnFunctions.structProduct(
                functions.array().cast(DataTypes.createArrayType(DataTypes.createStructType(
                    Arrays.asList(
                        DataTypes.createStructField("str", DataTypes.StringType, true),
                        DataTypes.createStructField("int", DataTypes.IntegerType, true)
                    )
                )))
            )
        )
        .withColumn("nonEmptyWithEmpty", ColumnFunctions.structProduct(
                functions.array(
                    functions.struct(
                        functions.lit("zzz").alias("nonEmpty")
                    ),
                    functions.struct(
                        functions.lit("yyy").alias("nonEmpty")
                    )
                ),
                functions.array().cast(DataTypes.createArrayType(DataTypes.createStructType(
                    Arrays.asList(
                        DataTypes.createStructField("str", DataTypes.StringType, true),
                        DataTypes.createStructField("int", DataTypes.IntegerType, true)
                    )
                )))
            )
        );

    final List<Row> results = resultDs.collectAsList();
    assertEquals(1, results.size());

    final Row row = results.get(0);
    assertEquals(0L, (Long) row.getAs("id"));
    assertNotNull(row.getAs("null"));
    assertNotNull(row.getAs("emptyArray"));
    assertNotNull(row.getAs("nonEmptyWithEmpty"));
  }
}

/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.encoders;

import org.apache.spark.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static au.csiro.pathling.encoders.ValueFunctions.*;

/**
 * Test for FHIR encoders.
 */
public class ExpressionsTest {


    private static SparkSession spark;

    /**
     * Set up Spark.
     */
    @BeforeAll
    public static void setUp() {
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
    public static void tearDown() {
        spark.stop();
    }

    public static Column size(Column arr) {
        return ifArray(arr, functions::size, x -> functions.when(arr.isNotNull(), functions.lit(1)).otherwise(functions.lit(0)));
    }

    @Test
    public void testIfArray() {
        Dataset<Row> ds = spark.range(2).toDF();
        Dataset<Row> resultDs = ds
                .withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
                .withColumn("test_single", ifArray(ds.col("id"), x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
                .withColumn("test_array", ifArray(functions.col("id_array"), x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
                .withColumn("test_array_with_unresolved", ifArray(functions.col("id_array"), x -> functions.array(functions.col("id_array")), x -> functions.lit(10)))
                .withColumn("test_lit", ifArray(functions.lit("a1"), x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
                .withColumn("test_array_lit", ifArray(functions.array(functions.lit("a1")), x -> functions.array(ds.col("id")), x -> ds.col("id")))
                .withColumn("test_array_lit_lambda", ifArray(functions.filter(functions.array(functions.lit("a1")), c -> c.equalTo("a1")), x -> functions.array(functions.lit(20)), x -> functions.lit(10)))
                .withColumn("test_array_size", size(functions.col("id_array")))
                .withColumn("test_array_where", ifArray(functions.col("id_array"), x -> functions.filter(x, c -> size(c).equalTo(functions.col("id"))), x -> functions.lit(0)));
        resultDs.show();
        resultDs.collectAsList().forEach(System.out::println);
    }

    @Test
    public void testFlatten() {
        Dataset<Row> ds = spark.range(2).toDF();
        Dataset<Row> resultDs = ds
                .withColumn("id_array", functions.array(functions.col("id"), functions.lit(22)))
                .withColumn("id_array_of_arrays", functions.array(functions.array(functions.col("id"), functions.lit(22)),
                        functions.array(functions.col("id"), functions.lit(33))))
                .withColumn("test_unnest_single", unnest(ds.col("id")))
                .withColumn("test_unnest_array", unnest(functions.col("id_array")))
                .withColumn("test_unnest_array_of_arrays", unnest(functions.col("id_array_of_arrays")))
                .withColumn("test_unnest_array_of_arrays_if_array", ifArray(functions.col("id_array"), x -> unnest(functions.col("id_array_of_arrays")), x -> x))
                .withColumn("test_unnest_array_of_arrays_if_id", ifArray(functions.col("id"), x -> unnest(functions.col("id_array_of_arrays")), x -> x));
        
        resultDs.show();
        resultDs.collectAsList().forEach(System.out::println);
    }

}

/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.SpringBootUnitTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
class SqlFunctionsTest {

  @Autowired
  private SparkSession spark;

  @Test
  void testPruneAnnotations() {
    // Create a schema with both regular and synthetic fields
    final StructType structSchema = new StructType()
        .add("id", DataTypes.StringType)
        .add("name", DataTypes.StringType)
        .add("_type", DataTypes.StringType)
        .add("_version", DataTypes.StringType)
        .add("value_scale", DataTypes.IntegerType);

    // Create a dataset with a struct column and primitive columns
    final Dataset<Row> dataset = spark.range(0)
        .toDF().select(
            SqlFunctions.prune_annotations(functions.lit(null).cast(structSchema)).alias("pruned"));

    final StructType prunedSchema = (StructType) dataset.schema().apply(0).dataType();
    final String[] expectedFieldNames = new String[]{"id", "name"};
    assertArrayEquals(expectedFieldNames, prunedSchema.fieldNames());
    for (int i = 0; i < expectedFieldNames.length; i++) {
      assertEquals(structSchema.apply(i), prunedSchema.apply(i));
    }
  }

  @Test
  void testPruneAnnotationsSimpleTypes() {
    // Test that simple types are not affected by prune_annotations
    final Dataset<Row> dataset = spark.range(1).toDF("id")
        .select(
            functions.col("id"),
            functions.lit("test string").as("string_val"),
            functions.lit(42).as("int_val"),
            functions.lit(3.14).as("double_val"),
            functions.lit(true).as("bool_val")
        );

    // Apply prune_annotations to all columns
    final Dataset<Row> result = dataset.select(
        SqlFunctions.prune_annotations(functions.col("id")).as("pruned_id"),
        SqlFunctions.prune_annotations(functions.col("string_val")).as("pruned_string"),
        SqlFunctions.prune_annotations(functions.col("int_val")).as("pruned_int"),
        SqlFunctions.prune_annotations(functions.col("double_val")).as("pruned_double"),
        SqlFunctions.prune_annotations(functions.col("bool_val")).as("pruned_bool")
    );

    // Verify that the values are unchanged
    final Row row = result.first();
    assertEquals(0L, row.getLong(0));
    assertEquals("test string", row.getString(1));
    assertEquals(42, row.getInt(2));
    assertEquals(3.14, row.getDouble(3), 0.0001);
    assertTrue(row.getBoolean(4));

    // Verify that the schema types are unchanged
    assertEquals(DataTypes.LongType, result.schema().apply("pruned_id").dataType());
    assertEquals(DataTypes.StringType, result.schema().apply("pruned_string").dataType());
    assertEquals(DataTypes.IntegerType, result.schema().apply("pruned_int").dataType());
    assertEquals(DataTypes.DoubleType, result.schema().apply("pruned_double").dataType());
    assertEquals(DataTypes.BooleanType, result.schema().apply("pruned_bool").dataType());
  }


  @Test
  void testToFhirInstant() {
    final Dataset<Row> result = spark.range(1).toDF()
        .select(
            SqlFunctions.to_fhir_instant(
                    functions.lit("2023-01-01T12:34:56.789Z").cast(DataTypes.TimestampType))
                .as("instant_utc"),
            SqlFunctions.to_fhir_instant(
                    functions.lit("2023-01-01T12:34:56.7+10:00").cast(DataTypes.TimestampType))
                .as("instant_plus_10")
        );
    final Row resultRow = result.first();

    assertEquals("2023-01-01T12:34:56.789Z", resultRow.getString(0));
    assertEquals("2023-01-01T02:34:56.700Z", resultRow.getString(1));
  }

  @Test
  void testToFhirInstantNull() {
    // Dataset with null timestamp
    final Dataset<Row> result = spark.range(1).toDF()
        .select(SqlFunctions.to_fhir_instant(functions.lit(null)).as("instant"));
    assertNull(result.first().get(0));
  }
}

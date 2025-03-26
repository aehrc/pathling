/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
public class SqlFunctionsTest {

  @Autowired
  private SparkSession spark;

  @Test
  void testNsMapConcat() {
    // Create a test dataset with different map combinations
    final List<Row> data = List.of(
        // Both maps have values
        RowFactory.create(
            Map.of("key1", 1, "key2", 2),
            Map.of("key3", 3, "key4", 4)
        ),
        // Left map is null
        RowFactory.create(
            null,
            Map.of("key5", 5, "key6", 6)
        ),
        // Right map is null
        RowFactory.create(
            Map.of("key7", 7, "key8", 8),
            null
        ),
        // Overlapping keys (key2 should be overwritten by right map)
        RowFactory.create(
            Map.of("key1", 1, "key2", 2),
            Map.of("key2", 20, "key3", 3)
        )
    );

    final StructType schema = new StructType()
        .add("left_map", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
        .add("right_map", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType));

    final Dataset<Row> df = spark.createDataFrame(data, schema);

    // Apply the ns_map_concat function
    final Dataset<Row> resultDf = df.select(
        functions.col("left_map"),
        functions.col("right_map"),
        SqlFunctions.ns_map_concat(
            functions.col("left_map"),
            functions.col("right_map")
        ).alias("concat_result")
    );

    // Expected results
    final List<Row> expectedData = List.of(
        // Both maps have values - concatenated
        RowFactory.create(
            Map.of("key1", 1, "key2", 2),
            Map.of("key3", 3, "key4", 4),
            Map.of("key1", 1, "key2", 2, "key3", 3, "key4", 4)
        ),
        // Left map is null - right map returned
        RowFactory.create(
            null,
            Map.of("key5", 5, "key6", 6),
            Map.of("key5", 5, "key6", 6)
        ),
        // Right map is null - left map returned
        RowFactory.create(
            Map.of("key7", 7, "key8", 8),
            null,
            Map.of("key7", 7, "key8", 8)
        ),
        // Overlapping keys - right map values take precedence
        RowFactory.create(
            Map.of("key1", 1, "key2", 2),
            Map.of("key2", 20, "key3", 3),
            Map.of("key1", 1, "key2", 20, "key3", 3)
        )
    );

    final StructType expectedSchema = new StructType()
        .add("left_map", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
        .add("right_map", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
        .add("concat_result", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType));

    final Dataset<Row> expectedDf = spark.createDataFrame(expectedData, expectedSchema);

    // Verify the results
    DatasetAssert.of(resultDf)
        .hasRows(expectedDf);
  }

  @Test
  void testCollectMap() {

    final List<Row> data = List.of(
        RowFactory.create("group1", Map.of("key1", 1, "key2", 2)),
        RowFactory.create("group1", Map.of("key2", 3, "key3", 4)),
        RowFactory.create("group2", Map.of("key1", 5)),
        RowFactory.create("group3", null)
    );

    final StructType schema = new StructType()
        .add("group", DataTypes.StringType)
        .add("map_column", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType));

    final Dataset<Row> df = spark.createDataFrame(data, schema);

    final Dataset<Row> aggregatedDf = df.groupBy("group")
        .agg(
            SqlFunctions.collect_map(functions.col("map_column"))
                .alias("map_column")
        );

    final Dataset<Row> expected = spark.createDataFrame(List.of(
            RowFactory.create("group1",
                Map.of("key1", 1, "key2", 3, "key3", 4)),
            RowFactory.create("group2", Map.of("key1", 5)),
            RowFactory.create("group3", null)),
        schema
    );

    DatasetAssert.of(aggregatedDf)
        .hasRows(expected);
  }

}

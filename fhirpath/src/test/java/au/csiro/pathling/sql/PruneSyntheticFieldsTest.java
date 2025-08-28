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

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.builders.DatasetBuilder;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
public class PruneSyntheticFieldsTest {

  @Autowired
  private SparkSession spark;

  private final Metadata metadata = Metadata.empty();
  private final StructType testStructType = DataTypes.createStructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, true, metadata),
      new StructField("name", DataTypes.StringType, true, metadata),
      new StructField("_fid", DataTypes.StringType, true, metadata)
  });

  @Test
  public void testPruneSyntheticFields() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn("active", DataTypes.BooleanType)
        .withColumn("gender", DataTypes.createArrayType(DataTypes.StringType))
        .withStructTypeColumns(testStructType)
        .withRow("patient-1", true, new String[]{"array_value-00-00"},
            RowFactory.create(1, "Test-1", "fid_value-00"))
        .withRow("patient-2", false, new String[]{"array_value-01-00", "array_value-01-01"},
            RowFactory.create(2, "Test-2", "fid_value_01"))
        .withRow("patient-3", null, null, null)
        .buildWithStructValue().repartition(1);

    // Prune all columns.
    final Dataset<Row> prunedDataset = dataset.select(
        Stream.of(dataset.columns()).map(dataset::col).map(SqlFunctions::prune_annotations)
            .toArray(Column[]::new));

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn("active", DataTypes.BooleanType)
        .withColumn("gender", DataTypes.createArrayType(DataTypes.StringType))
        .withStructColumn("id", DataTypes.IntegerType)
        .withStructColumn("name", DataTypes.StringType)
        .withRow("patient-1", true, new String[]{"array_value-00-00"},
            RowFactory.create(1, "Test-1"))
        .withRow("patient-2", false, new String[]{"array_value-01-00", "array_value-01-01"},
            RowFactory.create(2, "Test-2"))
        .withRow("patient-3", null, null, null)
        .buildWithStructValue().repartition(1);

    DatasetAssert.of(prunedDataset).hasRows(expectedResult);
  }

  @Test
  public void testPruneInGroupBy() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withStructTypeColumns(testStructType)
        .withRow("patient-1", "male", true, RowFactory.create(1, "Test-1", "fid-00"))
        .withRow("patient-2", "female", false, RowFactory.create(2, "Test-2", "fid-01"))
        .withRow("patient-3", "male", true, null)
        .withRow("patient-4", null, true, null)
        .withRow("patient-5", "female", false, RowFactory.create(2, "Test-2", "fid-02"))
        .buildWithStructValue().repartition(1);

    final Column valueColumn = dataset.col(dataset.columns()[dataset.columns().length - 1]);
    final Dataset<Row> groupedResult = dataset.groupBy(
            SqlFunctions.prune_annotations(valueColumn))
        .agg(functions.count(dataset.col("gender")));

    DatasetAssert.of(groupedResult)
        .hasRows(
            RowFactory.create(RowFactory.create(1, "Test-1"), 1),
            RowFactory.create(RowFactory.create(2, "Test-2"), 2),
            RowFactory.create(null, 1)
        );
  }
}

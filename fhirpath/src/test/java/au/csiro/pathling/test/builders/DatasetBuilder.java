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

package au.csiro.pathling.test.builders;

import static au.csiro.pathling.utilities.Preconditions.checkState;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Assists with the task of creating Datasets and lists of Rows for testing purposes.
 *
 * @author John Grimes
 */
@Slf4j
public class DatasetBuilder {

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final Metadata metadata;

  @Nonnull
  private final List<StructField> datasetColumns = new ArrayList<>();

  @Nonnull
  private final List<Row> datasetRows = new ArrayList<>();

  @Nonnull
  private final List<StructField> structColumns = new ArrayList<>();

  public DatasetBuilder(@Nonnull final SparkSession spark) {
    this.spark = spark;
    final Metadata newMetadata = new MetadataBuilder().build();
    requireNonNull(newMetadata);
    this.metadata = newMetadata;
  }

  @Nonnull
  public DatasetBuilder withColumn(@Nonnull final DataType dataType) {
    return withColumn(randomAlias(), dataType);
  }

  @Nonnull
  public DatasetBuilder withColumn(@Nonnull final String columnName,
      @Nonnull final DataType dataType) {
    final StructField column = new StructField(columnName, dataType, true, metadata);
    datasetColumns.add(column);
    return this;
  }

  @Nonnull
  public DatasetBuilder withIdColumn() {
    return withColumn(DataTypes.StringType);
  }

  @Nonnull
  public DatasetBuilder withIdColumn(@Nonnull final String alias) {
    return withColumn(alias, DataTypes.StringType);
  }

  @Nonnull
  public DatasetBuilder withRow(@Nonnull final Object... values) {
    final Row row = RowFactory.create(values);
    datasetRows.add(row);
    return this;
  }

  @Nonnull
  public DatasetBuilder changeValue(@Nonnull final String id, @Nonnull final Object value) {
    for (int i = 0; i < datasetRows.size(); i++) {
      final Row row = datasetRows.get(i);
      requireNonNull(row);

      final String string = row.getString(0);
      requireNonNull(string);

      if (string.equals(id)) {
        datasetRows.set(i, RowFactory.create(id, value));
      }
    }
    return this;
  }

  @SuppressWarnings("unused")
  @Nonnull
  public DatasetBuilder changeValues(@Nonnull final Object value,
      @Nonnull final Iterable<String> ids) {
    ids.forEach(id -> changeValue(id, value));
    return this;
  }

  @Nonnull
  public DatasetBuilder withStructColumn(@Nonnull final String name,
      @Nonnull final DataType dataType) {
    final StructField column = new StructField(name, dataType, true, metadata);
    structColumns.add(column);
    return this;
  }

  @Nonnull
  public DatasetBuilder withStructTypeColumns(@Nonnull final StructType structType) {
    final StructField[] fields = structType.fields();
    requireNonNull(fields);

    structColumns.addAll(Arrays.asList(fields));
    return this;
  }

  @Nonnull
  public Dataset<Row> build() {
    if (!structColumns.isEmpty()) {
      throw new RuntimeException(
          "Called build() on a DatasetBuilder with struct columns, did you mean to use "
              + "buildWithStructValue()?");
    } else if (datasetColumns.isEmpty()) {
      throw new RuntimeException("Called build() on a DatasetBuilder with no columns");
    }
    return getDataset(datasetColumns);
  }

  @Nonnull
  public Dataset<Row> buildWithStructValue() {
    final List<StructField> columns = new ArrayList<>(datasetColumns);
    columns.add(new StructField(
        randomAlias(),
        DataTypes.createStructType(structColumns),
        true,
        metadata));
    return getDataset(columns);
  }

  @Nonnull
  private Dataset<Row> getDataset(@Nonnull final List<StructField> columns) {
    final StructType schema;
    schema = new StructType(columns.toArray(new StructField[]{}));

    Dataset<Row> dataFrame = spark.createDataFrame(datasetRows, schema);
    requireNonNull(dataFrame);
    dataFrame = dataFrame.repartition(1);
    // needs to allow for empty datasets with 0 partitions
    checkState(dataFrame.rdd().getNumPartitions() <= 1,
        "at most one partition expected in test datasets constructed from rows, but got: "
            + dataFrame.rdd().getNumPartitions());
    return dataFrame;
  }

  @Nonnull
  public static DatasetBuilder of(@Nonnull final SparkSession spark) {
    return new DatasetBuilder(spark);
  }

}

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import static au.csiro.pathling.TestUtilities.getSparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

/**
 * Assists with the task of creating Datasets and lists of Rows for testing purposes.
 *
 * @author John Grimes
 */
public class DatasetBuilder {

  protected SparkSession spark;
  protected Metadata metadata;
  protected List<StructField> datasetColumns = new ArrayList<>();
  protected List<Row> datasetRows = new ArrayList<>();
  protected List<StructField> structColumns = new ArrayList<>();

  public DatasetBuilder() {
    spark = getSparkSession();
    metadata = new MetadataBuilder().build();
  }

  public DatasetBuilder withColumn(String name, DataType dataType) {
    StructField column = new StructField(name, dataType, true, metadata);
    datasetColumns.add(column);
    return this;
  }

  public DatasetBuilder withRow(Object... values) {
    Row row = RowFactory.create(values);
    datasetRows.add(row);
    return this;
  }

  public DatasetBuilder changeValue(String id, Object value) {
    for (int i = 0; i < datasetRows.size(); i++) {
      Row row = datasetRows.get(i);
      if (row.getString(0).equals(id)) {
        datasetRows.set(i, RowFactory.create(id, value));
      }
    }
    return this;
  }

  public DatasetBuilder withIdsAndValue(Object value, List<String> ids) {
    for (String id : ids) {
      Row row = RowFactory.create(id, value);
      datasetRows.add(row);
    }
    return this;
  }

  public DatasetBuilder withIdsAndNull(List<String> ids) {
    for (String id : ids) {
      Row row = RowFactory.create(id, null);
      datasetRows.add(row);
    }
    return this;
  }

  public DatasetBuilder withStructColumn(String name, DataType dataType) {
    StructField column = new StructField(name, dataType, true, metadata);
    structColumns.add(column);
    return this;
  }

  public DatasetBuilder withStructTypeColumns(StructType structType) {
    structColumns.addAll(Arrays.asList(structType.fields()));
    return this;
  }

  public Dataset<Row> build() {
    if (!structColumns.isEmpty()) {
      throw new RuntimeException(
          "Called build() on a DatasetBuilder with struct columns, did you mean to use "
              + "buildWithStructValue(String structName)?");
    } else if (datasetColumns.isEmpty()) {
      return null;
    }
    return getDataset(datasetColumns);
  }

  public Dataset<Row> buildWithStructValue(String structName) {
    List<StructField> columns = new ArrayList<>(datasetColumns);
    columns.add(new StructField(
        structName,
        DataTypes.createStructType(structColumns),
        true,
        metadata));
    return getDataset(columns);
  }

  public List<Row> getRows() {
    return datasetRows;
  }

  private Dataset<Row> getDataset(List<StructField> columns) {
    StructType schema;
    schema = new StructType(columns.toArray(new StructField[0]));
    return spark.createDataFrame(datasetRows, schema);
  }

}

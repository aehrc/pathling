/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.query.parsing.ParsedExpression;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * Assists with the task of creating ParsedExpressions for testing purposes.
 *
 * @author John Grimes
 */
public class ExpressionBuilder {

  protected ParsedExpression expression;
  protected DatasetBuilder datasetBuilder;
  protected Dataset<Row> dataset;

  public ExpressionBuilder() {
    expression = new ParsedExpression();
    datasetBuilder = new DatasetBuilder();
  }

  public ExpressionBuilder withDataset(Dataset<Row> dataset) {
    this.dataset = dataset;
    return this;
  }

  public ExpressionBuilder withColumn(String name, DataType dataType) {
    datasetBuilder.withColumn(name, dataType);
    return this;
  }

  public ExpressionBuilder withRow(Object... values) {
    datasetBuilder.withRow(values);
    return this;
  }

  public ExpressionBuilder withStructColumn(String name, DataType dataType) {
    datasetBuilder.withStructColumn(name, dataType);
    return this;
  }

  public ExpressionBuilder withStructTypeColumns(StructType structType) {
    datasetBuilder.withStructTypeColumns(structType);
    return this;
  }

  public ParsedExpression build() {
    if (dataset != null) {
      setDataset(dataset);
    } else {
      Dataset<Row> builtDataset = datasetBuilder.build();
      if (builtDataset != null) {
        setDataset(builtDataset);
      }
    }
    return expression;
  }

  public ParsedExpression buildWithStructValue(String structName) {
    if (dataset != null) {
      setDataset(dataset);
    } else {
      Dataset<Row> builtDataset = datasetBuilder.buildWithStructValue(structName);
      if (builtDataset != null) {
        setDataset(builtDataset);
      }
    }
    return expression;
  }

  private void setDataset(Dataset<Row> dataset) {
    expression.setDataset(dataset);
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column valueColumn = dataset.col(dataset.columns()[1]);
    expression.setIdColumn(idColumn);
    expression.setValueColumn(valueColumn);
  }

}

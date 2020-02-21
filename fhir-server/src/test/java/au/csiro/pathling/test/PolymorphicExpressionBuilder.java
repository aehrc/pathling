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

/**
 * @author John Grimes
 */
public class PolymorphicExpressionBuilder extends ExpressionBuilder {

  public PolymorphicExpressionBuilder() {
    super();
    expression.setResource(true);
    expression.setPolymorphic(true);
  }

  @Override
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

  @Override
  public ParsedExpression buildWithStructValue(String structName) {
    throw new RuntimeException(
        "Cannot use buildWithStructValue on a polymorphic expression builder");
  }

  private void setDataset(Dataset<Row> dataset) {
    expression.setDataset(dataset);
    Column idColumn = dataset.col(dataset.columns()[0]);
    Column typeColumn = dataset.col(dataset.columns()[1]);
    Column valueColumn = dataset.col(dataset.columns()[2]);
    expression.setIdColumn(idColumn);
    expression.setResourceTypeColumn(typeColumn);
    expression.setValueColumn(valueColumn);
  }

}

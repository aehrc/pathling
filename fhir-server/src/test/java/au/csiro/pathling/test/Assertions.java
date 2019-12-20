/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.query.parsing.ParsedExpression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Assertions {

  public static ParsedExpressionAssert assertThat(ParsedExpression parsedExpression) {
    return new ParsedExpressionAssert(parsedExpression);
  }

  public static DatasetAssert assertThat(Dataset<Row> rowDataset) {
    return new DatasetAssert(rowDataset);
  }


}

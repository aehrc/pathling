package au.csiro.pathling.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import au.csiro.pathling.query.parsing.ParsedExpression;

public class Assertions {

  public static ParsedExpressionAssert assertThat(ParsedExpression parsedExpression) {
    return new ParsedExpressionAssert(parsedExpression);
  }

  public static DatasetAssert assertThat(Dataset<Row> rowDataset) {
    return new DatasetAssert(rowDataset);
  }


}

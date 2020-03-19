/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.query.parsing.ParsedExpression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class Assertions {

  public static ParsedExpressionAssert assertThat(ParsedExpression parsedExpression) {
    return new ParsedExpressionAssert(parsedExpression);
  }

  public static DatasetAssert assertThat(Dataset<Row> rowDataset) {
    return new DatasetAssert(rowDataset);
  }

}

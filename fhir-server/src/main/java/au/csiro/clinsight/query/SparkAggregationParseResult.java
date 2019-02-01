/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import org.apache.spark.sql.types.DataType;

/**
 * @author John Grimes
 */
public class SparkAggregationParseResult {

  private String expression;
  private DataType resultType;
  private String fromTable;

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public DataType getResultType() {
    return resultType;
  }

  public void setResultType(DataType resultType) {
    this.resultType = resultType;
  }

  public String getFromTable() {
    return fromTable;
  }

  public void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

}

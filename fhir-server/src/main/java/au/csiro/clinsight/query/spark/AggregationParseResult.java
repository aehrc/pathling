/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import org.apache.spark.sql.types.DataType;

/**
 * @author John Grimes
 */
class AggregationParseResult {

  private String expression;
  private DataType resultType;
  private String fromTable;

  String getExpression() {
    return expression;
  }

  void setExpression(String expression) {
    this.expression = expression;
  }

  DataType getResultType() {
    return resultType;
  }

  void setResultType(DataType resultType) {
    this.resultType = resultType;
  }

  String getFromTable() {
    return fromTable;
  }

  void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

}

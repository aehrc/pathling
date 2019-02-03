/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

/**
 * @author John Grimes
 */
class GroupingParseResult {

  private String expression;
  private String fromTable;

  String getExpression() {
    return expression;
  }

  void setExpression(String expression) {
    this.expression = expression;
  }

  String getFromTable() {
    return fromTable;
  }

  void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

}

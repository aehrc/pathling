/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Used to represent the results from the AggregationParser and GroupingParser, which then gets used
 * to build a QueryPlan.
 *
 * @author John Grimes
 */
class ParseResult {

  private final SortedSet<Join> joins = new TreeSet<>();
  private String expression;
  private String resultType;
  private String fromTable;

  ParseResult(String expression) {
    this.expression = expression;
  }

  String getExpression() {
    return expression;
  }

  void setExpression(String expression) {
    this.expression = expression;
  }

  String getResultType() {
    return resultType;
  }

  void setResultType(String resultType) {
    this.resultType = resultType;
  }

  String getFromTable() {
    return fromTable;
  }

  void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

  SortedSet<Join> getJoins() {
    return joins;
  }

}

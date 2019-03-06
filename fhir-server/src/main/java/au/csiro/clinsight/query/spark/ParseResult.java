/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import java.util.HashSet;
import java.util.Set;
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
  private final Set<String> fromTable = new HashSet<>();
  private String sqlExpression;
  private String fhirPathExpression;
  private String resultTypeCode;
  private ResolvedElementType resultType;

  ParseResult() {
  }

  ParseResult(String sqlExpression) {
    this.sqlExpression = sqlExpression;
  }

  String getSqlExpression() {
    return sqlExpression;
  }

  void setSqlExpression(String sqlExpression) {
    this.sqlExpression = sqlExpression;
  }

  String getFhirPathExpression() {
    return fhirPathExpression;
  }

  void setFhirPathExpression(String fhirPathExpression) {
    this.fhirPathExpression = fhirPathExpression;
  }

  String getResultTypeCode() {
    return resultTypeCode;
  }

  void setResultTypeCode(String resultTypeCode) {
    this.resultTypeCode = resultTypeCode;
  }

  ResolvedElementType getResultType() {
    return resultType;
  }

  void setResultType(ResolvedElementType resultType) {
    this.resultType = resultType;
  }

  Set<String> getFromTable() {
    return fromTable;
  }

  SortedSet<Join> getJoins() {
    return joins;
  }

}

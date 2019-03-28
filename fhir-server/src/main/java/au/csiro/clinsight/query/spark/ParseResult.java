/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Used to represent the results from the AggregationParser and GroupingParser, which then gets used
 * to build a QueryPlan.
 *
 * @author John Grimes
 */
class ParseResult {

  @Nonnull
  private final SortedSet<Join> joins = new TreeSet<>();

  @Nonnull
  private final Set<String> fromTable = new HashSet<>();

  private String expression;
  private String sqlExpression;
  private ParseResultType resultType;
  private ResolvedElementType elementType;
  private String elementTypeCode;

  ParseResult() {
  }

  String getSqlExpression() {
    return sqlExpression;
  }

  void setSqlExpression(String sqlExpression) {
    this.sqlExpression = sqlExpression;
  }

  String getExpression() {
    return expression;
  }

  void setExpression(String expression) {
    this.expression = expression;
  }

  String getElementTypeCode() {
    return elementTypeCode;
  }

  void setElementTypeCode(String elementTypeCode) {
    this.elementTypeCode = elementTypeCode;
  }

  public ParseResultType getResultType() {
    return resultType;
  }

  public void setResultType(ParseResultType resultType) {
    this.resultType = resultType;
  }

  ResolvedElementType getElementType() {
    return elementType;
  }

  void setElementType(ResolvedElementType elementType) {
    this.elementType = elementType;
  }

  @Nonnull
  Set<String> getFromTable() {
    return fromTable;
  }

  @Nonnull
  SortedSet<Join> getJoins() {
    return joins;
  }

  public enum ParseResultType {
    ELEMENT_PATH, STRING_LITERAL
  }

}

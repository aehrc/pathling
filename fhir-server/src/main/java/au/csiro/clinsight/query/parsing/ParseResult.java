/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * Used to represent the results from the execution of an ExpressionParser, which then gets used to
 * build a QueryPlan.
 *
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class ParseResult {

  private final SortedSet<Join> joins = new TreeSet<>();
  private final Set<String> fromTables = new HashSet<>();

  @Nullable
  private String expression;

  @Nullable
  private String sqlExpression;

  @Nullable
  private String preAggregationExpression;

  @Nullable
  private ParseResultType resultType;

  @Nullable
  private ResolvedElementType elementType;

  @Nullable
  private String elementTypeCode;

  public SortedSet<Join> getJoins() {
    return joins;
  }

  public Set<String> getFromTables() {
    return fromTables;
  }

  @Nullable
  public String getExpression() {
    return expression;
  }

  public void setExpression(@Nullable String expression) {
    this.expression = expression;
  }

  @Nullable
  public String getSqlExpression() {
    return sqlExpression;
  }

  public void setSqlExpression(@Nullable String sqlExpression) {
    this.sqlExpression = sqlExpression;
  }

  @Nullable
  public String getPreAggregationExpression() {
    return preAggregationExpression;
  }

  public void setPreAggregationExpression(@Nullable String preAggregationExpression) {
    this.preAggregationExpression = preAggregationExpression;
  }

  @Nullable
  public ParseResultType getResultType() {
    return resultType;
  }

  public void setResultType(@Nullable ParseResultType resultType) {
    this.resultType = resultType;
  }

  @Nullable
  public ResolvedElementType getElementType() {
    return elementType;
  }

  public void setElementType(@Nullable ResolvedElementType elementType) {
    this.elementType = elementType;
  }

  @Nullable
  public String getElementTypeCode() {
    return elementTypeCode;
  }

  public void setElementTypeCode(@Nullable String elementTypeCode) {
    this.elementTypeCode = elementTypeCode;
  }

  public enum ParseResultType {
    ELEMENT_PATH, STRING_LITERAL, BOOLEAN
  }

}

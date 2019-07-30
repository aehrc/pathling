/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Used to represent the inputs to a FHIRPath function (ExpressionFunction).
 *
 * @author John Grimes
 */
public class ExpressionFunctionInput {

  /**
   * The results of parsing the expressions passed to this function as arguments.
   */
  private final List<ParseResult> arguments = new ArrayList<>();

  /**
   * Joins that will be needed to support the execution of the filter expression.
   */
  private final SortedSet<Join> filterJoins = new TreeSet<>();

  /**
   * The FHIRPath expression that invoked this function.
   */
  private String expression;

  /**
   * The result of parsing the input to this function, i.e. the expression preceding the period
   * within an invocation expression.
   */
  private ParseResult input;

  /**
   * The ExpressionParserContext that should be used to support the execution of this function.
   */
  private ExpressionParserContext context;

  /**
   * A SQL expression that should be used to limit the scope of the result.
   */
  private String filter;

  public List<ParseResult> getArguments() {
    return arguments;
  }

  public SortedSet<Join> getFilterJoins() {
    return filterJoins;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public ParseResult getInput() {
    return input;
  }

  public void setInput(ParseResult input) {
    this.input = input;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public ExpressionParserContext getContext() {
    return context;
  }

  public void setContext(ExpressionParserContext context) {
    this.context = context;
  }

}

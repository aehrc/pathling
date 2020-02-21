/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;

/**
 * Used to represent the inputs to the FHIRPath path traversal operator.
 *
 * @author John Grimes
 */
public class PathTraversalInput {

  /**
   * The result of parsing the left-hand input expression.
   */
  private ParsedExpression left;

  /**
   * The string representing the next path element to be traversed to.
   */
  private String right;

  /**
   * The FHIRPath expression that invoked this function.
   */
  private String expression;

  /**
   * The ExpressionParserContext that should be used to support the execution of this function.
   */
  private ExpressionParserContext context;

  public ParsedExpression getLeft() {
    return left;
  }

  public void setLeft(ParsedExpression left) {
    this.left = left;
  }

  public String getRight() {
    return right;
  }

  public void setRight(String right) {
    this.right = right;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public ExpressionParserContext getContext() {
    return context;
  }

  public void setContext(ExpressionParserContext context) {
    this.context = context;
  }

}

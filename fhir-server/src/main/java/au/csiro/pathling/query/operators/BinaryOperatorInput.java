/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;

/**
 * Used to represent the inputs to a binary FHIRPath operator.
 *
 * @author John Grimes
 */
public class BinaryOperatorInput {

  /**
   * The result of parsing the left-hand input expression.
   */
  private ParsedExpression left;

  /**
   * The result of parsing the right-hand input expression.
   */
  private ParsedExpression right;

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

  public ParsedExpression getRight() {
    return right;
  }

  public void setRight(ParsedExpression right) {
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

/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import au.csiro.pathling.query.parsing.ExpressionParserContext;
import au.csiro.pathling.query.parsing.ParsedExpression;
import java.util.ArrayList;
import java.util.List;

/**
 * Used to represent the inputs to a FHIRPath function.
 *
 * @author John Grimes
 */
public class FunctionInput {

  /**
   * The results of parsing the expressions passed to this function as arguments.
   */
  private final List<ParsedExpression> arguments = new ArrayList<>();

  /**
   * The result of parsing the input to this function, i.e. the expression preceding the period
   * within an invocation expression.
   */
  private ParsedExpression input;

  /**
   * The FHIRPath expression that invoked this function.
   */
  private String expression;

  /**
   * The ExpressionParserContext that should be used to support the execution of this function.
   */
  private ExpressionParserContext context;

  public ParsedExpression getInput() {
    return input;
  }

  public void setInput(ParsedExpression input) {
    this.input = input;
  }

  public List<ParsedExpression> getArguments() {
    return arguments;
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

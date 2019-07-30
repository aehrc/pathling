/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * @author John Grimes
 */
public class WhereFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    ParseResult argument = validateArgument(input.getArguments());

    // Get the function and inputs used to produce the input result.
    ExpressionFunction function = inputResult.getFunction();
    ExpressionFunctionInput functionInput = inputResult.getFunctionInput();

    // Unwind the aliases within the filter string, they will need to be re-applied when they find
    // their new home within the expression that they will modify.
    String filter = Join.unwindJoinAliases(argument.getSql(), argument.getJoins());

    // Modify the function inputs to include a filter expression and joins from the argument.
    functionInput.setFilter(filter);
    functionInput.getFilterJoins().addAll(argument.getJoins());

    // Return the modified parse result.
    ParseResult result = function.invoke(functionInput);
    result.setFunction(this);
    result.setFunctionInput(input);
    return result;
  }

  private ParseResult validateInput(ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for where function");
    }
    if (input.isSingular()) {
      throw new InvalidRequestException(
          "Cannot apply where function to expression that resolves to singular value");
    }
    return input;
  }

  private ParseResult validateArgument(@Nonnull List<ParseResult> arguments) {
    if (arguments.size() != 1) {
      throw new InvalidRequestException("One argument must be provided to where function");
    }
    return arguments.get(0);
  }

}

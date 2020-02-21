/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Describes a function which can scope down the previous invocation within a FHIRPath expression,
 * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
 * element currently in scope.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#where">where</a>
 */
public class WhereFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);

    Column idColumn,
        inputValueCol = inputResult.getValueColumn();
    Dataset<Row> dataset;
    if (argument.isLiteral()) {
      // Filter the input dataset based upon the literal value of the argument.
      dataset = inputResult.getDataset().filter(lit(argument.getJavaLiteralValue()));
      idColumn = inputResult.getIdColumn();
    } else {
      // Create a new dataset by performing an inner join from the input to the argument, based on
      // whether the boolean value is true or not.
      dataset = argument.getDataset().filter(argument.getValueColumn().equalTo(true));
      idColumn = argument.getIdColumn();
    }

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression(inputResult);
    result.setFhirPath(input.getExpression());
    result.setDataset(dataset);
    result.setHashedValue(idColumn, inputValueCol);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          "where function accepts one argument: " + input.getExpression());
    }
    ParsedExpression argument = input.getArguments().get(0);
    if (argument.getFhirPathType() != FhirPathType.BOOLEAN || !argument.isSingular()) {
      throw new InvalidRequestException(
          "Argument to where function must be a singular Boolean: " + argument.getFhirPath());
    }
  }

}

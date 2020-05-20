/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

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
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
 */
public class WhereFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);

    Column idColumn, valueColumn;
    Dataset<Row> dataset;
    if (argument.isLiteral()) {
      dataset = inputResult.getDataset();
      idColumn = inputResult.getIdColumn();
      // The result is the literal value if true, null otherwise.
      valueColumn = when(lit(argument.getJavaLiteralValue()), inputResult.getValueColumn())
          .otherwise(null);
    } else {
      // We use the argument dataset alone, to avoid the problems with joining from the input
      // dataset to the argument dataset (which would create duplicate rows).
      dataset = argument.getDataset();
      idColumn = argument.getIdColumn();
      // The result is the input value if it is equal to true, or null otherwise (signifying the
      // absence of a value).
      valueColumn = when(argument.getValueColumn().equalTo(true), inputResult.getValueColumn())
          .otherwise(null);
    }

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression(inputResult);
    result.setFhirPath(input.getExpression());
    result.setDataset(dataset);
    result.setHashedValue(idColumn, valueColumn);

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

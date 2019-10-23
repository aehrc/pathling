/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.utilities.Strings.md5Short;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
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
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#wherecriteria-expression-collection">http://hl7.org/fhirpath/2018Sep/index.html#wherecriteria-expression-collection</a>
 */
public class WhereFunction implements Function {

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull FunctionInput input) {
    validateInput(input);
    ParsedExpression inputResult = input.getInput();
    ParsedExpression argument = input.getArguments().get(0);
    String hash = md5Short(input.getExpression());

    // Create a new dataset by performing an inner join from the input to the argument, based on
    // whether the boolean value is true or not.
    Dataset<Row> inputDataset = inputResult.getDataset();
    Dataset<Row> argumentDataset = argument.getDataset();
    Column inputIdCol = inputResult.getIdColumn();
    Column inputValueCol = inputResult.getValueColumn();
    Column argumentIdCol = argument.getIdColumn();
    Column argumentValueCol = argument.getValueColumn();
    Dataset<Row> dataset = inputDataset
        .join(argumentDataset, inputIdCol.equalTo(argumentIdCol).and(argumentValueCol), "inner");

    dataset = dataset.select(inputIdCol, inputValueCol);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression(inputResult);
    result.setDataset(dataset);
    result.setIdColumn(inputIdCol);
    result.setValueColumn(inputValueCol);

    return result;
  }

  private void validateInput(FunctionInput input) {
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          "where function accepts one argument: " + input.getExpression());
    }
    ParsedExpression argument = input.getArguments().get(0);
    if (argument.getFhirPathType() != FhirPathType.BOOLEAN) {
      throw new InvalidRequestException(
          "Argument to where function must be Boolean: " + argument.getFhirPath());
    }
  }

}

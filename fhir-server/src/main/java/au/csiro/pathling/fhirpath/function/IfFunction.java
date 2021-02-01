/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import java.util.Arrays;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This function takes three arguments, Returns the second argument if the first argument evaluates
 * to {@code true}, or the third argument otherwise.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#iif">iif</a>
 */
public class IfFunction implements NamedFunction {

  private static final String NAME = "iif";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(input.getArguments().size() == 3,
        "3 arguments must be supplied to iif function");
    final FhirPath condition = input.getArguments().get(0);
    checkUserInput(condition instanceof BooleanPath,
        "Condition argument to iif must be Boolean: " + condition.getExpression());
    checkUserInput(condition.isSingular(),
        "Condition argument to iif must be singular: " + condition.getExpression());
    final BooleanPath conditionBoolean = (BooleanPath) condition;
    checkUserInput(conditionBoolean.getThisColumn().isPresent(),
        "Condition argument to iif function must be navigable from collection item (use $this): "
            + conditionBoolean.getExpression());
    final FhirPath ifTrue = input.getArguments().get(1);
    final FhirPath otherwise = input.getArguments().get(2);
    final FHIRDefinedType ifTrueType = getFhirTypeForArgument(ifTrue);
    final FHIRDefinedType otherwiseType = getFhirTypeForArgument(otherwise);
    checkUserInput(ifTrueType == otherwiseType,
        "ifTrue and otherwise argument to iif must be of the same type");

    // Join the three datasets together and create a value column.
    final Dataset<Row> dataset = join(input.getContext(),
        Arrays.asList(conditionBoolean, ifTrue, otherwise), JoinType.LEFT_OUTER);
    final Column valueColumn =
        when(conditionBoolean.getValueColumn().equalTo(true), ifTrue.getValueColumn())
            .otherwise(otherwise.getValueColumn());

    // Build a new ElementPath based on the type of the literal `ifTrue` and `otherwise` arguments,
    // and populate it with the dataset and calculated value column.
    final String expression = expressionFromInput(input, NAME);
    return ElementPath
        .build(expression, dataset, inputPath.getIdColumn(), inputPath.getEidColumn(), valueColumn,
            inputPath.isSingular(), inputPath.getForeignResource(), inputPath.getThisColumn(),
            ifTrueType);
  }

  private FHIRDefinedType getFhirTypeForArgument(final FhirPath argument) {
    final FHIRDefinedType fhirType;
    if (argument instanceof ElementPath) {
      fhirType = ((ElementPath) argument).getFhirType();
    } else if (argument instanceof LiteralPath) {
      fhirType = LiteralPath.fhirPathToFhirType(((LiteralPath) argument).getClass());
    } else {
      throw new InvalidUserInputError(
          "Argument not supported within iif function: " + argument.getExpression());
    }
    return fhirType;
  }

}

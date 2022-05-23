/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.DatePath;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.sql.dates.TemporalDifferenceFunction;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public class UntilFunction implements NamedFunction {

  private static final String NAME = "until";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getArguments().size() == 2,
        "until function must have two arguments");
    final NonLiteralPath fromArgument = input.getInput();
    final FhirPath toArgument = input.getArguments().get(0);
    final FhirPath calendarDurationArgument = input.getArguments().get(1);

    checkUserInput(fromArgument instanceof DateTimePath || fromArgument instanceof DatePath,
        "until function must be invoked on a DateTime or Date");
    checkUserInput(toArgument instanceof DateTimePath || toArgument instanceof DateTimeLiteralPath
            || toArgument instanceof DatePath || toArgument instanceof DateLiteralPath,
        "until function must have a DateTime or Date as the first argument");
    checkUserInput(calendarDurationArgument instanceof StringLiteralPath,
        "until function must have a String as the second argument");
    final String literalValue = ((StringLiteralPath) calendarDurationArgument).getValue()
        .asStringValue();
    checkUserInput(TemporalDifferenceFunction.isValidCalendarDuration(literalValue),
        "Invalid calendar duration: " + literalValue);

    final Dataset<Row> dataset = join(input.getContext(), fromArgument, toArgument,
        JoinType.LEFT_OUTER);
    final Column valueColumn = callUDF(TemporalDifferenceFunction.FUNCTION_NAME,
        fromArgument.getValueColumn(), toArgument.getValueColumn(),
        calendarDurationArgument.getValueColumn());
    final String expression = NamedFunction.expressionFromInput(input, NAME);

    return ElementPath.build(expression, dataset, fromArgument.getIdColumn(),
        fromArgument.getEidColumn(), valueColumn, fromArgument.isSingular(),
        fromArgument.getCurrentResource(), fromArgument.getThisColumn(), FHIRDefinedType.INTEGER);
  }

}

/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.NonLiteralPath.findEidColumn;
import static au.csiro.pathling.fhirpath.NonLiteralPath.findThisColumn;
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
import au.csiro.pathling.sql.misc.TemporalDifferenceFunction;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This function computes the time interval (duration) between two paths representing dates or dates
 * with time.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#until">until</a>
 */
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

    checkUserInput(fromArgument.isSingular(),
        "until function must be invoked on a singular path");
    checkUserInput(toArgument.isSingular(),
        "until function must have the singular path as its first argument");

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

    final Optional<Column> eidColumn = findEidColumn(fromArgument, toArgument);
    final Optional<Column> thisColumn = findThisColumn(List.of(fromArgument, toArgument));

    return ElementPath.build(expression, dataset, fromArgument.getIdColumn(),
        eidColumn, valueColumn, true,
        fromArgument.getCurrentResource(), thisColumn, FHIRDefinedType.INTEGER);
  }

}

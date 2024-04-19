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

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
 */
public class CountFunction extends AggregateFunction implements NamedFunction {

  private static final String NAME = "count";

  protected CountFunction() {
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments("count", input);
    final NonLiteralPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, NAME);
    final Column subjectColumn = inputPath.getValueColumn();

    // When we are counting resources from the input context, we use the distinct count to account
    // for the fact that there may be duplicate IDs in the dataset.
    // When we are counting anything else, we use a non-distinct count, to account for the fact that
    // it is valid to have multiple of the same value.
    final Function<Column, Column> countFunction = inputPath == input.getContext().getInputContext()
                                                   ? functions::countDistinct
                                                   : functions::count;

    // According to the FHIRPath specification, the count function must return 0 when invoked on an
    // empty collection.
    final Column valueColumn = when(countFunction.apply(subjectColumn).isNull(), 0L)
        .otherwise(countFunction.apply(subjectColumn));

    return buildAggregateResult(inputPath.getDataset(), input.getContext(), inputPath, valueColumn,
        expression, FHIRDefinedType.UNSIGNEDINT);
  }

}

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
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Nesting;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
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
    final Nesting nesting = input.getContext().getNesting();

    final Column aggregateColumn;
    final UnaryOperator<Column> valueColumnProducer;
    if (nesting.isEmpty()) {
      final Column subjectColumn = inputPath.getValueColumn();
      // When we are counting anything else, we use a non-distinct count, to account for the fact 
      // that it is valid to have multiple of the same value.
      aggregateColumn = count(subjectColumn);
      valueColumnProducer = UnaryOperator.identity();
    } else {
      // Use the ordering columns if they are present, otherwise use the value column (which should 
      // only ever be a resource ID).
      final List<Column> orderingColumns = nesting.getOrderingColumns();
      if (orderingColumns.isEmpty()) {
        aggregateColumn = countDistinct(inputPath.getValueColumn());
      } else {
        final List<Column> countColumns = new ArrayList<>();
        countColumns.add(inputPath.getIdColumn());
        countColumns.addAll(orderingColumns);
        final Column first = checkPresent(countColumns.stream().limit(1).findFirst());
        final Column[] remaining = countColumns.stream().skip(1).toArray(Column[]::new);
        aggregateColumn = countDistinct(first, remaining);
      }
      // When we are counting values within an unnested dataset, we use a distinct count to account
      // for the fact that there may be duplicates.
      valueColumnProducer = UnaryOperator.identity();
    }

    return buildAggregateResult(inputPath.getDataset(), input.getContext(), inputPath,
        aggregateColumn, valueColumnProducer, expression, FHIRDefinedType.UNSIGNEDINT);
  }

}

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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This function returns true if the input collection is empty.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#empty">empty</a>
 */
public class EmptyFunction implements NamedFunction {

  private static final String NAME = "empty";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments(NAME, input);
    final NonLiteralPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, NAME);

    // We use the count function to determine whether there are zero items in the input collection.
    final FhirPath countResult = new CountFunction().invoke(input);
    final Dataset<Row> dataset = countResult.getDataset();
    final Column valueColumn = countResult.getValueColumn().equalTo(0);

    return ElementPath
        .build(expression, dataset, inputPath.getIdColumn(), Optional.empty(), valueColumn, true,
            Optional.empty(), inputPath.getThisColumn(), FHIRDefinedType.BOOLEAN);
  }

}

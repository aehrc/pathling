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
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.not;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Returns {@code true} if the input collection evaluates to {@code false}, and {@code false} if it
 * evaluates to {@code true}. Otherwise, the result is empty.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#not">not</a>
 */
public class NotFunction implements NamedFunction {

  private static final String NAME = "not";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments(NAME, input);
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(inputPath instanceof BooleanPath,
        "Input to not function must be Boolean: " + inputPath.getExpression());
    final String expression = expressionFromInput(input, NAME);

    // The not function is just a thin wrapper over the Spark not function.
    final Column valueColumn = not(inputPath.getValueColumn());

    return ElementPath
        .build(expression, inputPath.getDataset(), inputPath.getIdColumn(),
            inputPath.getEidColumn(), valueColumn, inputPath.isSingular(), Optional.empty(),
            inputPath.getThisColumn(), FHIRDefinedType.BOOLEAN);
  }

}

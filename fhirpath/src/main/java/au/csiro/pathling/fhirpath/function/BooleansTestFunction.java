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
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.Column;

/**
 * @author John Grimes
 */
public class BooleansTestFunction extends AggregateFunction implements NamedFunction {

  @Nonnull
  private final BooleansTestType type;

  /**
   * @param type The type of Boolean collection test
   */
  public BooleansTestFunction(@Nonnull final BooleansTestType type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments(type.getFunctionName(), input);
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(inputPath instanceof BooleanPath,
        "Input to " + type + " function must be Boolean");
    final Column inputColumn = inputPath.getValueColumn();
    final String expression = expressionFromInput(input, type.getFunctionName());

    final Column valueColumn = type.getEquality().apply(inputColumn);
    return buildAggregateResult(inputPath.getDataset(), input.getContext(), inputPath, valueColumn,
        expression);
  }

  /**
   * Represents a type of test that can be performed against a collection of Boolean values.
   */
  @AllArgsConstructor
  @Getter
  public enum BooleansTestType {
    /**
     * "Any true" test
     */
    ANY_TRUE("anyTrue", input -> max(coalesce(input, lit(false))).equalTo(lit(true))),
    /**
     * "Any false" test
     */
    ANY_FALSE("anyFalse", input -> min(coalesce(input, lit(true))).equalTo(lit(false))),
    /**
     * "All true" test
     */
    ALL_TRUE("allTrue", input -> min(coalesce(input, lit(true))).equalTo(lit(true))),
    /**
     * "All false" test
     */
    ALL_FALSE("allFalse", input -> max(coalesce(input, lit(false))).equalTo(lit(false)));

    @Nonnull
    private final String functionName;

    @Nonnull
    private final UnaryOperator<Column> equality;

    @Override
    public String toString() {
      return functionName;
    }

  }

}

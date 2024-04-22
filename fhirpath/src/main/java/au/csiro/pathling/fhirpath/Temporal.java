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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Describes a path that represents a temporal value such as DateTime or Date, and can be the
 * subject of date arithmetic operations involving time durations.
 *
 * @author John Grimes
 */
public interface Temporal {

  /**
   * Gets a function that can take the {@link QuantityLiteralPath} representing a time duration and
   * return a {@link FhirPath} that contains the result of date arithmetic operation for this path
   * and the provided duration. The type of operation is controlled by supplying a
   * {@link MathOperation}.
   *
   * @param operation The {@link MathOperation} type to retrieve a result for
   * @param dataset The {@link Dataset} to use within the result
   * @param expression The FHIRPath expression to use within the result
   * @return A {@link Function} that takes a {@link QuantityLiteralPath} as its parameter, and
   * returns a {@link FhirPath}.
   */
  @Nonnull
  Function<QuantityLiteralPath, FhirPath> getDateArithmeticOperation(
      @Nonnull MathOperation operation, @Nonnull Dataset<Row> dataset,
      @Nonnull String expression);

  /**
   * Gets a function that can take the {@link QuantityLiteralPath} representing a time duration and
   * return a {@link FhirPath} that contains the result of applying the date arithmetic operation
   * for to the source path and the provided duration. The type of operation is controlled by
   * supplying a {@link MathOperation}.
   *
   * @param source the {@link FhirPath} to which the operation should be applied to. Should be a
   * {@link Temporal} path.
   * @param operation The {@link MathOperation} type to retrieve a result for
   * @param dataset The {@link Dataset} to use within the result
   * @param expression the FHIRPath expression to use within the result
   * @param additionFunctionName the name of the UDF to use for additions.
   * @param subtractionFunctionName the name of the UDF to use for subtractions.
   * @return A {@link Function} that takes a {@link QuantityLiteralPath} as its parameter, and
   * returns a {@link FhirPath}.
   */
  @Nonnull
  static Function<QuantityLiteralPath, FhirPath> buildDateArithmeticOperation(
      @Nonnull final FhirPath source, final @Nonnull MathOperation operation,
      final @Nonnull Dataset<Row> dataset, final @Nonnull String expression,
      final String additionFunctionName, final String subtractionFunctionName) {
    return target -> {
      final String functionName;
      final Optional<Column> eidColumn = NonLiteralPath.findEidColumn(source, target);
      final Optional<Column> thisColumn = NonLiteralPath.findThisColumn(List.of(source, target));

      switch (operation) {
        case ADDITION:
          functionName = additionFunctionName;
          break;
        case SUBTRACTION:
          functionName = subtractionFunctionName;
          break;
        default:
          throw new AssertionError("Unsupported date arithmetic operation: " + operation);
      }

      final Column valueColumn = functions.callUDF(functionName, source.getValueColumn(),
          target.getValueColumn());
      return ElementPath.build(expression, dataset, source.getIdColumn(), eidColumn, valueColumn,
          true,
          Optional.empty(), thisColumn, FHIRDefinedType.DATETIME);
    };
  }

}

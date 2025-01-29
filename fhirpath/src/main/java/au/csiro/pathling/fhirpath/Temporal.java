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
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import jakarta.annotation.Nonnull;
import java.util.function.Function;

/**
 * Describes a path that represents a temporal value such as DateTime or Date, and can be the
 * subject of date arithmetic operations involving time durations.
 *
 * @author John Grimes
 */
public interface Temporal {

  /**
   * Gets a function that can take the {@link QuantityCollection} representing a time duration and
   * return a {@link Collection} that contains the result of date arithmetic operation for this path
   * and the provided duration. The type of operation is controlled by supplying a
   * {@link MathOperation}.
   *
   * @param operation The {@link MathOperation} type to retrieve a result for
   * @return A {@link Function} that takes a {@link QuantityCollection} as its parameter, and
   * returns a {@link Collection}.
   */
  @Nonnull
  Function<QuantityCollection, Collection> getDateArithmeticOperation(
      @Nonnull MathOperation operation);

  /**
   * Gets a function that can take the {@link QuantityCollection} representing a time duration and
   * return a {@link Collection} that contains the result of applying the date arithmetic operation
   * for to the source path and the provided duration. The type of operation is controlled by
   * supplying a {@link MathOperation}.
   *
   * @param source the {@link Collection} to which the operation should be applied to. Should be a
   * {@link Temporal} path.
   * @param operation The {@link MathOperation} type to retrieve a result for
   * @param additionFunctionName the name of the UDF to use for additions.
   * @param subtractionFunctionName the name of the UDF to use for subtractions.
   * @return A {@link Function} that takes a {@link QuantityCollection} as its parameter, and
   * returns a {@link Collection}.
   */
  @Nonnull
  static Function<QuantityCollection, Collection> buildDateArithmeticOperation(
      @Nonnull final Collection source, final @Nonnull MathOperation operation,
      final String additionFunctionName,
      final String subtractionFunctionName) {
    return target -> {
      final String functionName;

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
      return DateTimeCollection.build(
          source.getColumn().singular().callUdf(functionName, target.getColumn().singular()));
    };
  }

}

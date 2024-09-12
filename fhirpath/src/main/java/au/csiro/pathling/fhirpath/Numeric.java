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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.Getter;
import org.apache.spark.sql.Column;

/**
 * Describes a path that represents a numeric value, and can be the subject of math operations.
 *
 * @author John Grimes
 */
public interface Numeric {

  /**
   * Get a function that can take two Numeric paths and return a {@link Collection} that contains
   * the result of a math operation. The type of operation is controlled by supplying a
   * {@link MathOperation}.
   *
   * @param operation The {@link MathOperation} type to retrieve a result for
   * @return A {@link Function} that takes a Numeric as its parameter, and returns a
   * {@link Collection}.
   */
  @Nonnull
  Function<Numeric, Collection> getMathOperation(@Nonnull MathOperation operation);

  /**
   * @return a {@link Column} within the dataset containing the values of the nodes
   */
  @Nonnull
  ColumnRepresentation getColumn();

  /**
   * @return a {@link Column} that provides a value that can me used in math operations
   */
  @Nonnull
  Optional<Column> getNumericValue();

  /**
   * Provides a {@link Column} that provides additional context that informs the way that math
   * operations are carried out. This is used for Quantity math, so that the operation function has
   * access to the canonicalized units.
   *
   * @return a {@link Column} that provides additional context for math operations
   */
  @Nonnull
  Optional<Column> getNumericContext();

  /**
   * The type of the result of evaluating this expression, if known.
   */
  @Nonnull
  Optional<FhirPathType> getType();

  /**
   * Represents a type of math operator.
   */
  enum MathOperation {
    /**
     * Addition operator.
     */
    ADDITION("+", Column::plus),
    /**
     * Subtraction operator.
     */
    SUBTRACTION("-", Column::minus),
    /**
     * Multiplication operator.
     */
    MULTIPLICATION("*", Column::multiply),
    /**
     * Division operator.
     */
    DIVISION("/", Column::divide),
    /**
     * Modulus operator.
     */
    MODULUS("mod", Column::mod);

    @Nonnull
    private final String fhirPath;

    /**
     * A Spark function that can be used to execute this type of math operation for simple types.
     * Complex types such as Quantity will implement their own math operation functions.
     */
    @Nonnull
    @Getter
    private final BiFunction<Column, Column, Column> sparkFunction;

    MathOperation(@Nonnull final String fhirPath,
        @Nonnull final BiFunction<Column, Column, Column> sparkFunction) {
      this.fhirPath = fhirPath;
      this.sparkFunction = sparkFunction;
    }

    @Override
    public String toString() {
      return fhirPath;
    }

  }
}

/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.comparison;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;

/**
 * An interface for FHIRPath elements that support equality operations.
 *
 * @author John Grimes
 */
public interface Equatable {

  /**
   * @return the comparator to use for equality comparisons
   */
  @Nonnull
  default ColumnEquality getComparator() {
    return DefaultComparator.getInstance();
  }

  /**
   * Determines whether this element is comparable to another collection.
   *
   * @param other the other collection
   * @return true if the two collections are comparable, otherwise false
   */
  default boolean isComparableTo(@Nonnull final Collection other) {
    if (getType().isPresent() || other.getType().isPresent()) {
      return getType().equals(other.getType());
    } else if (other instanceof EmptyCollection) {
      return true;
    } else {
      throw new UnsupportedFhirPathFeatureError("Unsupported equality for complex types");
    }
  }

  /**
   * Get a function that can take two Equatable paths and return a {@link ColumnRepresentation} that
   * contains an element equality condition. The type of condition is controlled by supplying
   * a{@link EqualityOperation}.
   *
   * @param operation The {@link EqualityOperation} type to retrieve the equality for
   * @return A {@link Function} that takes an Equatable as its parameter, and returns a {@link
   *     ColumnRepresentation}
   */
  @Nonnull
  default Function<Equatable, ColumnRepresentation> getElementEquality(
      @Nonnull final EqualityOperation operation) {
    return target -> {
      final Column columnResult =
          operation.equalityFunction.apply(
              getComparator(),
              getColumn().singular("Element equality requires singular left operand").getValue(),
              target.getColumn().singular("Element equality singular right operand").getValue());
      return new DefaultRepresentation(columnResult);
    };
  }

  /**
   * Gets the FHIR path type of this element if it has one.
   *
   * @return the FHIR path type of this element if it has one.
   */
  @Nonnull
  Optional<FhirPathType> getType();

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return A {@link Column}
   */
  @Nonnull
  ColumnRepresentation getColumn();

  /** Represents a type of comparison operation. */
  enum EqualityOperation {
    /** The equals operation. */
    EQUALS("=", ColumnEquality::equalsTo),

    /** The not equals operation. */
    NOT_EQUALS("!=", ColumnEquality::notEqual);

    @Nonnull private final String fhirPath;

    @Nonnull private final TriFunction<ColumnEquality, Column, Column, Column> equalityFunction;

    EqualityOperation(
        @Nonnull final String fhirPath,
        @Nonnull final TriFunction<ColumnEquality, Column, Column, Column> equalityFunction) {
      this.fhirPath = fhirPath;
      this.equalityFunction = equalityFunction;
    }

    @Override
    @Nonnull
    public String toString() {
      return fhirPath;
    }

    /**
     * Binds a {@link ColumnEquality} to this operation, returning a function that takes two {@link
     * Column} objects and produces a {@link Column} containing the result of the comparison.
     *
     * @param comparator the comparator to bind
     * @return a function that takes two columns and produces a comparison result column
     */
    @Nonnull
    public BinaryOperator<Column> bind(@Nonnull final ColumnEquality comparator) {
      return (a, b) -> equalityFunction.apply(comparator, a, b);
    }
  }
}

/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.Comparable.ComparisonOperation;
import jakarta.annotation.Nonnull;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e.
 * {@code <=}, {@code <}, {@code >}, {@code >=}.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#comparison">Comparison</a>
 */
public class ComparisonOperator implements FhirPathBinaryOperator {

  @Nonnull
  private final ComparisonOperation type;

  /**
   * @param type The type of operator
   */
  public ComparisonOperator(@Nonnull final ComparisonOperation type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    final Collection left = input.left();
    final Collection right = input.right();

    // If either operand is an EmptyCollection, return an EmptyCollection.
    if (left instanceof EmptyCollection || right instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    checkUserInput(left instanceof Comparable,
        "Left operand to " + type + " operator must be comparable");
    checkUserInput(right instanceof Comparable,
        "Right operand to " + type + " operator must be comparable");

    // actual comparison operations require both sides to be comparable with each other
    checkUserInput(left.isComparableTo(right),
        "Comparison of paths is not supported: " + left.getDisplayExpression() + ", "
            + right.getDisplayExpression());
    final Collection leftSingular = left.asSingular(
        "Comparison operator requires singular values");

    check(leftSingular instanceof Comparable);
    return BooleanCollection.build(
        ((Comparable) leftSingular).getComparison(type)
            .apply((Comparable) right));
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}

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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import javax.annotation.Nonnull;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e. {@code =},
 * {@code !=}, {@code <=}, {@code <}, {@code >}, {@code >=}.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#equality">Equality</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#comparison">Comparison</a>
 */
@NotImplemented
public class ComparisonOperator implements BinaryOperator {

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
    final Collection left = input.getLeft();
    final Collection right = input.getRight();
    checkUserInput(left.isComparableTo(right), "Operands must be comparable");

    return BooleanCollection.build(
        ColumnRepresentation.binaryOperator(left.getColumn(), right.getColumn(),
            (l, r) -> left.getComparison(type).apply(right))
    );
  }

}

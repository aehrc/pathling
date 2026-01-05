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

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.Comparable.ComparisonOperation;
import jakarta.annotation.Nonnull;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e. {@code
 * <=}, {@code <}, {@code >}, {@code >=}.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#comparison">Comparison</a>
 */
public class ComparisonOperator extends SameTypeBinaryOperator {

  @Nonnull private final ComparisonOperation type;

  /**
   * @param type The type of operator
   */
  public ComparisonOperator(@Nonnull final ComparisonOperation type) {
    this.type = type;
  }

  @Override
  @Nonnull
  protected Collection handleEquivalentTypes(
      @Nonnull final Collection left,
      @Nonnull final Collection right,
      @Nonnull final BinaryOperatorInput input) {
    if (!(left instanceof Comparable && right instanceof Comparable)) {
      return fail(input);
    }
    final Collection leftSingular = left.asSingular("Comparison operator requires singular values");
    return BooleanCollection.build(
        ((Comparable) leftSingular).getComparison(type).apply((Comparable) right));
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}

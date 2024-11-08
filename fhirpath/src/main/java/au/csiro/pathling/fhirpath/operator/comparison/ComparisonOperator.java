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

package au.csiro.pathling.fhirpath.operator.comparison;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.rendering.SingleColumnRendering;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorInput;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#equality">FHIRPath Specification - Equality</a>
 * @see <a href="https://hl7.org/fhirpath/#comparison">FHIRPath Specification - Comparison</a>
 */
public class ComparisonOperator implements BinaryOperator {

  private final @NotNull ComparisonOperation type;

  /**
   * @param type The type of operator
   */
  public ComparisonOperator(final @NotNull ComparisonOperation type) {
    this.type = type;
  }

  @Override
  public @NotNull Collection invoke(final @NotNull BinaryOperatorInput input) {
    final Collection left = input.getLeft().evaluate(input.getInput(), input.getContext())
        .singleton();
    final Collection right = input.getRight().evaluate(input.getInput(), input.getContext())
        .singleton();

    // If we know the types of both operands and they are not the same, we throw an error.
    if (left.getType().isPresent() && right.getType().isPresent()) {
      final FhirPathType leftType = left.getType().get();
      final FhirPathType rightType = right.getType().get();
      if (!leftType.equals(rightType)) {
        throw new UnsupportedOperationException(
            "Comparison not supported between " + leftType + " and "
                + rightType + " types");
      }
    }

    final ColumnComparator comparator = left.compare();
    final Column result = type.getOperation()
        .apply(comparator, left.getRendering(), right.getRendering());
    return new BooleanCollection(new SingleColumnRendering(result),
        Optional.of(FhirPathType.BOOLEAN));
  }

}

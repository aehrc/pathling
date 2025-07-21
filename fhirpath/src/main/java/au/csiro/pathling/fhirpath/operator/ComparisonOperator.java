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

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.Comparable.ComparisonOperation;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

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

    // If either operand is an EmptyCollection, return an EmptyCollection.
    if (left instanceof EmptyCollection || right instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    checkUserInput(left instanceof au.csiro.pathling.fhirpath.comparison.Comparable,
        "Left operand to " + type + " operator must be comparable");
    checkUserInput(right instanceof au.csiro.pathling.fhirpath.comparison.Comparable,
        "Right operand to " + type + " operator must be comparable");
    final au.csiro.pathling.fhirpath.comparison.Comparable leftComparable = (au.csiro.pathling.fhirpath.comparison.Comparable) left;
    final au.csiro.pathling.fhirpath.comparison.Comparable rightComparable = (au.csiro.pathling.fhirpath.comparison.Comparable) right;

    String leftDisplay = left.getType().map(FhirPathType::getTypeSpecifier).orElse("unknown");
    final Optional<FHIRDefinedType> leftFhirType = left.getFhirType();
    if (leftFhirType.isPresent()) {
      leftDisplay += " (" + leftFhirType.get().toCode() + ")";
    }
    String rightDisplay = right.getType().map(FhirPathType::getTypeSpecifier).orElse("unknown");
    final Optional<FHIRDefinedType> rightFhirType = right.getFhirType();
    if (rightFhirType.isPresent()) {
      rightDisplay += " (" + rightFhirType.get().toCode() + ")";
    }
    checkUserInput(leftComparable.isComparableTo(rightComparable),
        "Comparison of paths is not supported: " + leftDisplay + ", " + rightDisplay);

    // If the comparison operation is equality, we can use the operands directly. If it is any other
    // type of comparison, we need to enforce that the operands are both singular values.
    if (type == ComparisonOperation.EQUALS || type == ComparisonOperation.NOT_EQUALS) {
      return BooleanCollection.build(leftComparable.getComparison(type).apply(rightComparable));
    } else {
      final Collection leftSingular = left.asSingular(
          "Comparison operator requires singular values");
      check(leftSingular instanceof au.csiro.pathling.fhirpath.comparison.Comparable);
      return BooleanCollection.build(
          ((au.csiro.pathling.fhirpath.comparison.Comparable) leftSingular).getComparison(type)
              .apply((Comparable) right));
    }

  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}

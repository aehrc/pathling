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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateTimeComparable;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import jakarta.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Provides the functionality of the family of math operators within FHIRPath, i.e. +, -, *, / and
 * mod.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#math">Math</a>
 */
public class MathOperator implements FhirPathBinaryOperator {

  private static final String NON_SINGULAR_ERROR_FORMAT =
      "Math operator (%s) requires the %s operand to be singular.";

  @Nonnull private final MathOperation type;

  /**
   * @param type The type of math operation
   */
  public MathOperator(@Nonnull final MathOperation type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {

    final Collection leftValue = input.left();
    final Collection rightValue = input.right();

    // manual selection of the types
    if ((leftValue instanceof DateTimeComparable || leftValue instanceof TimeCollection)
        && rightValue instanceof QuantityCollection qty) {
      return invokeDateTime(leftValue, qty);
    }
    final Pair<Collection, Collection> reconciledArguments =
        FhirPathBinaryOperator.reconcileTypes(leftValue, rightValue);

    final Collection left =
        reconciledArguments
            .getLeft()
            .asSingular(NON_SINGULAR_ERROR_FORMAT.formatted(type.toString(), "left"));
    final Collection right =
        reconciledArguments
            .getRight()
            .asSingular(NON_SINGULAR_ERROR_FORMAT.formatted(type.toString(), "right"));

    if (left instanceof QuantityCollection || right instanceof QuantityCollection) {
      throw new UnsupportedFhirPathFeatureError(
          "Quantity arithmetic operations are not yet supported");
    }

    checkUserInput(
        left instanceof Numeric,
        type + " operator does not support left operand: " + left.getDisplayExpression());
    checkUserInput(
        right instanceof Numeric,
        type + " operator does not support right operand: " + right.getDisplayExpression());

    checkUserInput(
        left.isComparableTo(right),
        "Left and right operands are not comparable: "
            + left.getDisplayExpression()
            + " "
            + type
            + " "
            + right.getDisplayExpression());

    final Numeric leftNumeric = (Numeric) left;
    final Numeric rightNumeric = (Numeric) right;
    return leftNumeric.getMathOperation(type).apply(rightNumeric);
  }

  @Nonnull
  private Collection invokeDateTime(
      @Nonnull final Collection ignoredLeftValue,
      @Nonnull final QuantityCollection ignoredRightValue) {
    throw new UnsupportedFhirPathFeatureError("Unsupported dateTime arithmetic");
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}

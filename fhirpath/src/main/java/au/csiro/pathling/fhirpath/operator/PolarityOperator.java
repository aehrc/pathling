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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.utilities.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Represents a polarity operator in FHIRPath.
 *
 * @param negation True for '-' operator, false for '+' operator
 * @author Piotr Szul
 */
public record PolarityOperator(boolean negation) implements UnaryOperator {

  @Override
  @Nonnull
  public Collection invoke(@Nullable final UnaryOperatorInput input) {
    requireNonNull(input);
    Preconditions.checkUserInput(
        input.input() instanceof Numeric, "Polarity operator can only be applied to numeric types");
    final Collection singularInput =
        input.input().asSingular("Polarity operator (-) requires a singular operand.");
    return negation ? ((Numeric) singularInput).negate() : singularInput;
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return negation ? "-" : "+";
  }

  /**
   * Returns a new instance of {@link PolarityOperator} from the specified polarity symbol ('+' or
   * '-').
   *
   * @param symbol The polarity symbol to use
   * @return A new instance of {@link PolarityOperator}
   */
  @Nonnull
  public static PolarityOperator fromSymbol(@Nonnull final String symbol) {
    if (symbol.equals("+")) {
      return new PolarityOperator(false);
    } else if (symbol.equals("-")) {
      return new PolarityOperator(true);
    } else {
      throw new IllegalArgumentException("Invalid symbol: " + symbol);
    }
  }
}

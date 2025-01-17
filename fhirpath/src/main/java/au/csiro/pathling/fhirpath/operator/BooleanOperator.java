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
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the family of boolean operators within FHIRPath, i.e. and, or, xor
 * and implies.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#boolean-logic">Boolean
 * logic</a>
 */
public class BooleanOperator implements BinaryOperator {

  private final BooleanOperatorType type;

  /**
   * @param type The type of operator
   */
  public BooleanOperator(final BooleanOperatorType type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    final Collection left = input.getLeft();
    final Collection right = input.getRight();

    checkUserInput(left instanceof BooleanCollection,
        "Left operand to " + type + " operator must be Boolean");
    checkUserInput(right instanceof BooleanCollection,
        "Right operand to " + type + " operator must be Boolean");

    final ColumnRepresentation resultCtx = ColumnRepresentation.binaryOperator(left.getColumn(),
        right.getColumn(),
        (leftValue, rightValue) -> {
          // Based on the type of operator, create the correct column expression.
          final Column valueColumn;
          switch (type) {
            case AND:
              valueColumn = leftValue.and(rightValue);
              break;
            case OR:
              valueColumn = leftValue.or(rightValue);
              break;
            case XOR:
              valueColumn = when(
                  leftValue.isNull().or(rightValue.isNull()), null
              ).when(
                  leftValue.equalTo(true).and(rightValue.equalTo(false)).or(
                      leftValue.equalTo(false).and(rightValue.equalTo(true))
                  ), true
              ).otherwise(false);
              break;
            case IMPLIES:
              valueColumn = when(
                  leftValue.equalTo(true), rightValue
              ).when(
                  leftValue.equalTo(false), true
              ).otherwise(
                  when(rightValue.equalTo(true), true)
                      .otherwise(null)
              );
              break;
            default:
              throw new AssertionError("Unsupported boolean operator encountered: " + type);
          }
          return valueColumn;
        });
    return BooleanCollection.build(resultCtx);
  }

  /**
   * Represents a type of Boolean operator.
   */
  public enum BooleanOperatorType {
    /**
     * AND operation.
     */
    AND("and"),
    /**
     * OR operation.
     */
    OR("or"),
    /**
     * Exclusive OR operation.
     */
    XOR("xor"),
    /**
     * Material implication.
     */
    IMPLIES("implies");

    @Nonnull
    private final String fhirPath;

    BooleanOperatorType(@Nonnull final String fhirPath) {
      this.fhirPath = fhirPath;
    }

    @Override
    public String toString() {
      return fhirPath;
    }

  }
  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}

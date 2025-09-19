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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.comparison.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.comparison.Equatable.EqualityOperation;
import au.csiro.pathling.fhirpath.operator.BooleanOperator.BooleanOperatorType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.Getter;

/**
 * All supported operators in FHIRPath.
 *
 * @author John Grimes
 */
@Getter
public enum BinaryOperatorType {

  /**
   * Logical AND operator.
   */
  AND("and", new BooleanOperator(BooleanOperatorType.AND)),
  /**
   * Logical OR operator.
   */
  OR("or", new BooleanOperator(BooleanOperatorType.OR)),
  /**
   * Logical exclusive OR operator.
   */
  XOR("xor", new BooleanOperator(BooleanOperatorType.XOR)),
  /**
   * Logical implication operator.
   */
  IMPLIES("implies", new BooleanOperator(BooleanOperatorType.IMPLIES)),
  /**
   * Equality comparison operator.
   */
  EQUALS("=", new EqualityOperator(EqualityOperation.EQUALS)),
  /**
   * Inequality comparison operator.
   */
  NOT_EQUALS("!=", new EqualityOperator(EqualityOperation.NOT_EQUALS)),
  /**
   * Less than or equal to comparison operator.
   */
  LESS_THAN_OR_EQUAL_TO("<=", new ComparisonOperator(ComparisonOperation.LESS_THAN_OR_EQUAL_TO)),
  /**
   * Less than comparison operator.
   */
  LESS_THAN("<", new ComparisonOperator(ComparisonOperation.LESS_THAN)),
  /**
   * Greater than or equal to comparison operator.
   */
  GREATER_THAN_OR_EQUAL_TO(">=",
      new ComparisonOperator(ComparisonOperation.GREATER_THAN_OR_EQUAL_TO)),
  /**
   * Greater than comparison operator.
   */
  GREATER_THAN(">", new ComparisonOperator(ComparisonOperation.GREATER_THAN)),
  /**
   * Arithmetic addition operator.
   */
  ADDITION("+", new MathOperator(MathOperation.ADDITION)),
  /**
   * Arithmetic subtraction operator.
   */
  SUBTRACTION("-", new MathOperator(MathOperation.SUBTRACTION)),
  /**
   * Arithmetic multiplication operator.
   */
  MULTIPLICATION("*", new MathOperator(MathOperation.MULTIPLICATION)),
  /**
   * Arithmetic division operator.
   */
  DIVISION("/", new MathOperator(MathOperation.DIVISION)),
  /**
   * Arithmetic modulus operator.
   */
  MODULUS("mod", new MathOperator(MathOperation.MODULUS));

  private final String symbol;

  private final FhirPathBinaryOperator instance;

  /**
   * Mapping of operator symbols to instances of those operators.
   */
  private static final Map<String, BinaryOperatorType> SYMBOL_TO_TYPE;

  static {
    final Builder<String, BinaryOperatorType> builder = new ImmutableMap.Builder<>();
    for (final BinaryOperatorType type : BinaryOperatorType.values()) {
      builder.put(type.getSymbol(), type);
    }
    SYMBOL_TO_TYPE = builder.build();
  }

  BinaryOperatorType(@Nonnull final String symbol, @Nonnull final FhirPathBinaryOperator instance) {
    this.symbol = symbol;
    this.instance = instance;
  }

  /**
   * Retrieves an instance of the operator with the specified symbol.
   *
   * @param symbol The symbol of the operator
   * @return The BinaryOperatorType corresponding to the specified symbol
   * @throws IllegalArgumentException if no operator with the specified symbol is supported
   */
  @Nonnull
  public static BinaryOperatorType fromSymbol(@Nonnull final String symbol)
      throws IllegalArgumentException {
    final BinaryOperatorType operatorType = SYMBOL_TO_TYPE.get(symbol);
    if (operatorType == null) {
      throw new UnsupportedFhirPathFeatureError("Unsupported operator: " + symbol);
    }
    return operatorType;
  }

  // #01 . (path/function invocation)
  // #02 [] (indexer)
  // #03 unary + and -
  // #04: *, /, div, mod
  // #05: +, -, &
  // #06: is, as
  // #07: |
  // #08: >, <, >=, <=
  // #09: =, ~, !=, !~
  // #10: in, contains
  // #11: and
  // #12: xor, or
  // #13: implies  

  private static final Map<String, Integer> PRECEDENCE =
      new ImmutableMap.Builder<String, Integer>()
          .put("*", 4)
          .put("/", 4)
          .put("div", 4)
          .put("mod", 4)
          .put("+", 5)
          .put("-", 5)
          .put("&", 5)
          .put("is", 6)
          .put("as", 6)
          .put("|", 7)
          .put(">", 8)
          .put("<", 8)
          .put(">=", 8)
          .put("<=", 8)
          .put("=", 9)
          .put("~", 9)
          .put("!=", 9)
          .put("!~", 9)
          .put("in", 10)
          .put("contains", 10)
          .put("and", 11)
          .put("xor", 12)
          .put("or", 12)
          .put("implies", 13)
          .build();

  /**
   * Compares the precedence of two operators. The result is negative if {@code op1} higher
   * precedence than {@code op2}, zero if they have the same precedence, and positive if {@code op1}
   * has lower precedence than {@code op2}.
   * <p>
   * Operators with higher precedence are evaluated before operators with lower and can be used in
   * expressions without parentheses.
   * <p>
   * For example, {@code comparePrecedence("*", "+")} returns a negative value because the
   * multiplication operator has higher precedence than the addition operator. This means that the
   * expression {@code 1 + 2 * 3} is evaluated as {@code 1 + (2 * 3)}.
   *
   * @param op1 the symbol of the first operator
   * @param op2 the symbol of the second operator
   * @return an integer representing the relative precedence of the two operators
   */
  public static int comparePrecedence(@Nonnull final String op1,
      @Nonnull final String op2) {
    return PRECEDENCE.get(op1) - PRECEDENCE.get(op2);
  }
}

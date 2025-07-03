package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.operator.BooleanOperator.BooleanOperatorType;
import au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation;
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

  AND("and", new BooleanOperator(BooleanOperatorType.AND)),
  OR("or", new BooleanOperator(BooleanOperatorType.OR)),
  XOR("xor", new BooleanOperator(BooleanOperatorType.XOR)),
  IMPLIES("implies", new BooleanOperator(BooleanOperatorType.IMPLIES)),
  EQUALS("=", new ComparisonOperator(ComparisonOperation.EQUALS)),
  NOT_EQUALS("!=", new ComparisonOperator(ComparisonOperation.NOT_EQUALS)),
  LESS_THAN_OR_EQUAL_TO("<=", new ComparisonOperator(ComparisonOperation.LESS_THAN_OR_EQUAL_TO)),
  LESS_THAN("<", new ComparisonOperator(ComparisonOperation.LESS_THAN)),
  GREATER_THAN_OR_EQUAL_TO(">=",
      new ComparisonOperator(ComparisonOperation.GREATER_THAN_OR_EQUAL_TO)),
  GREATER_THAN(">", new ComparisonOperator(ComparisonOperation.GREATER_THAN)),
  ADDITION("+", new MathOperator(MathOperation.ADDITION)),
  SUBTRACTION("-", new MathOperator(MathOperation.SUBTRACTION)),
  MULTIPLICATION("*", new MathOperator(MathOperation.MULTIPLICATION)),
  DIVISION("/", new MathOperator(MathOperation.DIVISION)),
  MODULUS("mod", new MathOperator(MathOperation.MODULUS)),
  COMBINE("combine", new CombineOperator());

  private final String symbol;

  private final BinaryOperator instance;

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

  BinaryOperatorType(@Nonnull final String symbol, @Nonnull final BinaryOperator instance) {
    this.symbol = symbol;
    this.instance = instance;
  }

  /**
   * Retrieves an instance of the operator with the specified symbol.
   *
   * @param symbol The symbol of the operator
   * @return The {@link BinaryOperatorType} corresponding to the specified symbol
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
          .put("combine", 7)
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

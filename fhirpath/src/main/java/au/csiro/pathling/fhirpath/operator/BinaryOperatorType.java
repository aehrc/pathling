package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.operator.BooleanOperator.BooleanOperatorType;
import au.csiro.pathling.fhirpath.operator.MembershipOperator.MembershipOperatorType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Map;
import javax.annotation.Nonnull;
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
  IN("in", new MembershipOperator(MembershipOperatorType.IN)),
  CONTAINS("contains", new MembershipOperator(MembershipOperatorType.CONTAINS)),
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
      throw new IllegalArgumentException("Unsupported operator: " + symbol);
    }
    return operatorType;
  }

}

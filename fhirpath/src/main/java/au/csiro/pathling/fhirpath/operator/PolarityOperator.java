package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.utilities.Preconditions;
import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * Represents a polarity operator in FHIRPath.
 *
 * @author Piotr Szul
 */
@Value
public class PolarityOperator implements UnaryOperator {

  /**
   * True for '-' operator, false for '+' operator.
   */
  boolean negation;

  @Override
  @Nonnull
  public Collection invoke(@Nonnull final UnaryOperatorInput input) {
    Preconditions.checkUserInput(input.getInput() instanceof Numeric,
        "Polarity operator can only be applied to numeric types");
    final Collection singularInput = input.getInput().asSingular();
    return negation
           ? ((Numeric) singularInput).negate()
           : singularInput;
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return negation
           ? "-"
           : "+";
  }
  
  /**
   * Returns a new instance of {@link PolarityOperator} from the specified polarity symbol ('+' or
   * '-').
   *
   * @param symbol The polarity symbol to use
   * @return A new instance of {@link PolarityOperator}
   */
  @Nonnull
  public static PolarityOperator fromSymbol(@Nonnull String symbol) {
    if (symbol.equals("+")) {
      return new PolarityOperator(false);
    } else if (symbol.equals("-")) {
      return new PolarityOperator(true);
    } else {
      throw new IllegalArgumentException("Invalid symbol: " + symbol);
    }
  }
}

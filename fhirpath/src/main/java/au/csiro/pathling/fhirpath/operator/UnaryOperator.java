package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;

/**
 * Represents a unary operator in FHIRPath.
 *
 * @author Piotr Szul
 */
public interface UnaryOperator {

  /**
   * Represents the input to a unary operator.
   *
   * @param input A {@link Collection} object representing the input to the operator
   */
  record UnaryOperatorInput(@Nonnull Collection input) {

  }

  /**
   * Invokes this operator with the specified input.
   *
   * @param input An {@link UnaryOperatorInput} object
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  Collection invoke(UnaryOperatorInput input);

  /**
   * Gets the name of this operator.
   *
   * @return the operator name, defaults to the simple class name
   */
  default String getOperatorName() {
    return this.getClass().getSimpleName();
  }

}

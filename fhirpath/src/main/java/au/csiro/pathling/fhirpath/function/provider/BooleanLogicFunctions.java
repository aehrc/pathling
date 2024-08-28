package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Contains functions for boolean logic.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#boolean-logic">FHIRPath Specification - Boolean logic</a>
 */
public class BooleanLogicFunctions {

  /**
   * Returns {@code true} if the input collection evaluates to {@code false}, and {@code false} if
   * it evaluates to {@code true}.
   * <p>
   * Otherwise, the result is empty.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the negated values
   * @see <a href="https://hl7.org/fhirpath/#not--boolean">not</a>
   */
  @FhirPathFunction
  public static BooleanCollection not(final Collection input) {
    final Column column = functions.not(input.getColumn());
    return new BooleanCollection(column, Optional.of(FhirPathType.BOOLEAN));
  }

}

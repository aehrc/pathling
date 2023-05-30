package au.csiro.pathling.fhirpath;

import javax.annotation.Nonnull;

/**
 * Describes a path that can be coerced to a String representation.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/N1/#tostring-string">toString</a>
 */
public interface StringCoercible {

  /**
   * @param expression the expression for the new path
   * @return a new {@link FhirPath} representing the String representation of this path
   */
  @Nonnull
  FhirPath asStringPath(@Nonnull String expression);

}

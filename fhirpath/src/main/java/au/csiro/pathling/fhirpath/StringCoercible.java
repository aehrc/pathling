package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;

/**
 * Describes a path that can be coerced to a String representation.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/N1/#tostring-string">toString</a>
 */
public interface StringCoercible {

  /**
   * @return a new {@link Collection} representing the String representation of this path
   */
  @Nonnull
  StringCollection asStringPath();

}

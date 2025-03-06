package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
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

  /**
   * Returns a new {@link StringCollection} representing the String representation of the input
   * using the Spark SQL cast to string type.
   *
   * @param collection The input collection
   * @return A new {@link StringCollection} representing the String representation of the input
   */
  static StringCollection defaultAsStringPath(@Nonnull final Collection collection) {
    return collection.map(ColumnRepresentation::asString, StringCollection::build);
  }
  
}

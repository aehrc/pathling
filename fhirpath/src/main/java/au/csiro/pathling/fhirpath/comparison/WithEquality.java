package au.csiro.pathling.fhirpath.comparison;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * An interface for FHIRPath elements that support equality comparisons.
 *
 * @author John Grimes
 */
public interface WithEquality {


  /**
   * @return the comparator to use for equality comparisons
   */
  @Nonnull
  default ColumnEquality getComparator() {
    return DefaultComparator.getInstance();
  }

  /**
   * Determines whether this element is comparable to another collection.
   *
   * @param other the other collection
   * @return true if the two collections are comparable, otherwise false
   */
  default boolean isComparableTo(@Nonnull final Collection other) {
    if (getType().isPresent() || other.getType().isPresent()) {
      return getType().equals(other.getType());
    } else if (other instanceof EmptyCollection) {
      return true;
    } else {
      throw new UnsupportedFhirPathFeatureError("Unsupported equality for complex types");
    }
  }

  /**
   * Gets the FHIR path type of this element if it has one.
   *
   * @return the FHIR path type of this element if it has one.
   */
  @Nonnull
  Optional<FhirPathType> getType();

}

package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.Reference;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;

/**
 * Represents a polymorphic resource collection, which can be resolved to a single resource type.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Value
@EqualsAndHashCode(callSuper = false)
public class MixedResourceCollection extends MixedCollection {

  @Nonnull
  Reference reference;

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @return A new collection representing just the elements of this collection with the specified
   * type
   */
  @Nonnull
  @Override
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    return reference.resolve(type.toResourceType());
  }

}

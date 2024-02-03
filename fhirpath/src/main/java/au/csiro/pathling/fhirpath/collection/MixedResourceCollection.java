package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.Reference;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;

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
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    return reference.resolve(type.toResourceType());
  }
 
}

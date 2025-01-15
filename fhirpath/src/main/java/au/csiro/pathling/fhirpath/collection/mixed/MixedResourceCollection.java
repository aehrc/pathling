package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.function.BiFunction;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a polymorphic resource collection, which can be resolved to a single resource type.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Value
@EqualsAndHashCode(callSuper = false)
public class MixedResourceCollection extends MixedCollection {

  ReferenceCollection resourceCollection;

  BiFunction<ReferenceCollection, ResourceType, ResourceCollection> resolver;

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
    return resolver.apply(resourceCollection, type.toResourceType());
  }

  // TODO: maybe override map() instead of these two methods

  @Override
  @Nonnull
  public Collection copyWith(@NotNull final ColumnRepresentation newValue) {
    return new MixedResourceCollection((ReferenceCollection) resourceCollection.copyWith(newValue),
        resolver);
  }

  @Override
  @Nonnull
  public ColumnRepresentation getColumn() {
    return resourceCollection.getColumn();
  }

  @Override
  public boolean convertibleTo(@Nonnull final Collection other) {
    // TODO: Perhpaps more needs to be done to the check the compatibility of the resource types
    //  as well as possibly compatibility of the resolved resource collections
    return other instanceof MixedResourceCollection;
  }

}

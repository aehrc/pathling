package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.Reference;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.NullRepresentation;
import au.csiro.pathling.fhirpath.definition.ChoiceChildDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Represents a polymorphic collection, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
@Getter
public abstract class MixedCollection extends Collection {

  protected MixedCollection() {
    super(NullRepresentation.getInstance(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param definition The definition to use
   * @return A new instance of {@link MixedCollection}
   */
  @Nonnull
  public static MixedCollection buildElement(@Nonnull final Collection parent,
      @Nonnull final ChoiceChildDefinition definition) {
    return new ChoiceElementCollection(definition, parent);
  }


  @Nonnull
  public static MixedCollection buildResource(@Nonnull final Reference reference) {
    return new MixedResourceCollection(reference);
  }


  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    // This should technically result in unchecked traversal, but we don't currently have a way
    // of implementing this.
    // See: https://hl7.org/fhirpath/#paths-and-polymorphic-items
    throw new UnsupportedOperationException(
        "Direct traversal of polymorphic collections is not supported."
            + " Please use 'ofType()' to specify the type of element to traverse.");
  }

}
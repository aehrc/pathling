package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.UnsupportedRepresentation;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;

/**
 * Represents a polymorphic collection, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
@Getter
public abstract class MixedCollection extends Collection {

  /**
   * Creates a new MixedCollection with an unsupported description.
   *
   * @param unsupportedDescription the description for unsupported operations
   */
  protected MixedCollection(@Nonnull final String unsupportedDescription) {
    super(new UnsupportedRepresentation(unsupportedDescription),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Creates a new MixedCollection with a default unsupported description.
   */
  @SuppressWarnings("unused")
  protected MixedCollection() {
    this("mixed collection (do you need to use ofType?)");
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param parent The parent collection
   * @param definition The definition to use
   * @return A new instance of MixedCollection
   */
  @Nonnull
  public static MixedCollection buildElement(@Nonnull final Collection parent,
      @Nonnull final ChoiceDefinition definition) {
    return new ChoiceElementCollection(definition, parent);
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

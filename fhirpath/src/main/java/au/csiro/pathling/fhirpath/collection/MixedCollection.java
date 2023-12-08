package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.Reference;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.definition.ChoiceChildDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Value;

/**
 * Represents a polymorphic collection, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
@Getter
public abstract class MixedCollection extends Collection {

  @Value
  public static class Element extends MixedCollection {

    /**
     * The definition of this choice element.
     */
    @Nonnull
    ChoiceChildDefinition choiceDefinition;

    @Nonnull
    Collection parent;

    /**
     * Returns a new collection representing just the elements of this collection with the specified
     * type.
     *
     * @param type The type of element to return
     * @return A new collection representing just the elements of this collection with the specified
     * type
     */
    @Nonnull
    public Collection resolveType(@Nonnull final TypeSpecifier type) {
      return choiceDefinition.getChildByType(type.toFhirType().toCode()).map(
          parent::traverseElement
      ).orElse(Collection.nullCollection());
    }
  }

  @Value
  public static class Resource extends MixedCollection {

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
    public Collection resolveType(@Nonnull final TypeSpecifier type) {
      return reference.resolve(type.toResourceType());
    }
  }

  protected MixedCollection() {
    super(ColumnCtx.nullCtx(), Optional.empty(), Optional.empty(), Optional.empty());
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
    return new Element(definition, parent);
  }


  @Nonnull
  public static MixedCollection buildResource(@Nonnull final Reference reference) {
    return new Resource(reference);
  }


  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    // FHIRPATH_NOTE: this should technically result in unchecked traversal
    throw new UnsupportedOperationException(
        "Direct traversal of polymorphic collections is not supported."
            + " Please use 'ofType()' to specify the type of element to traverse.");
  }

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @return A new collection representing just the elements of this collection with the specified
   * type
   */
  @Nonnull
  abstract public Collection resolveType(@Nonnull final TypeSpecifier type);
}

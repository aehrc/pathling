package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.definition.ChoiceChildDefinition;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = false)
public class ChoiceElementCollection extends MixedCollection {

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
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    return choiceDefinition.getChildByType(type.toFhirType().toCode()).map(
        parent::traverseElement
    ).orElse(Collection.nullCollection());
  }

}

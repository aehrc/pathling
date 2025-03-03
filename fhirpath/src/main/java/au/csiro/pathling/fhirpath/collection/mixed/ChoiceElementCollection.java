package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = false)
public class ChoiceElementCollection extends MixedCollection {

  /**
   * The definition of this choice element.
   */
  @Nonnull
  ChoiceDefinition choiceDefinition;

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
  @Override
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    if (!type.isFhirType()) {
      return super.filterByType(type);
    }
    return choiceDefinition.getChildByType(type.toFhirType().toCode())
        .map(parent::traverseElement)
        .orElse(super.filterByType(type));
  }

}

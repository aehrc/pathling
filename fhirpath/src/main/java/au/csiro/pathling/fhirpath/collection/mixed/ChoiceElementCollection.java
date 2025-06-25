package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

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
    if (type.isSystemType()) {
      return resolveSystemType(type.toSystemType());
    } else if (type.isFhirType()) {
      return resolveFhirType(type.toFhirType());
    } else {
      return EmptyCollection.getInstance();
    }
  }

  @Nonnull
  private Collection resolveFhirType(@Nonnull final FHIRDefinedType fhirType) {
    return resolveElement(choiceDefinition.getChildByType(fhirType.toCode()));
  }

  @Nonnull
  private Collection resolveSystemType(@Nonnull final FhirPathType fhirpathType) {

    // find the list of FHIRDefinedTypes that match this FhirpathType
    final List<FHIRDefinedType> fhirPathTypes = fhirpathType.getFhirTypes();
    // find the element definitions that match the FHIRDefinedTypes in this choice element
    final ElementDefinition[] selectedTypes = fhirPathTypes.stream()
        .map(FHIRDefinedType::toCode)
        .flatMap(ft -> choiceDefinition.getChildByType(ft).stream())
        .toArray(ElementDefinition[]::new);

    if (selectedTypes.length <= 1) {
      // delegate to the single choice mapping
      // this will take care of Decimal case (creation of DecimalCollection and DecimalRepsentation)
      return resolveElement(Arrays.stream(selectedTypes).findFirst());
    } else {
      return Collection.build(parent.getColumn().traverseChoice(selectedTypes),
          fhirpathType.getDefaultFhirType());
    }
  }

  @Nonnull
  private Collection resolveElement(@Nonnull final Optional<ElementDefinition> elementDefinition) {
    // This method resolves the element definition to a collection.
    // It is used to traverse the choice element.
    return elementDefinition
        .map(parent::traverseElement)
        .orElse(EmptyCollection.getInstance());
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation delegates to parent collection.
   */
  @Nonnull
  @Override
  public BooleanCollection asBooleanSingleton() {
    return parent.asBooleanSingleton();
  }
}

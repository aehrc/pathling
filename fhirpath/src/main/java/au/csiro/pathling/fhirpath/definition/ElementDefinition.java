package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildAny;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.BackboneElement;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public interface ElementDefinition extends NodeDefinition<ElementDefinition> {

  /**
   * @param childDefinition A HAPI {@link BaseRuntimeChildDefinition} that describes this element
   * @return A shiny new ElementDefinition
   */
  @Nonnull
  static ElementDefinition build(@Nonnull final BaseRuntimeChildDefinition childDefinition) {
    if (childDefinition instanceof RuntimeChildAny && "valueReference".equals(childDefinition
        .getElementName())) {
      return new ReferenceExtensionDefinition((RuntimeChildAny) childDefinition);
    } else if (childDefinition instanceof RuntimeChildResourceDefinition) {
      return new ReferenceDefinition((RuntimeChildResourceDefinition) childDefinition);
    } else if (childDefinition instanceof RuntimeChildChoiceDefinition) {
      return new ChoiceElementDefinition((RuntimeChildChoiceDefinition) childDefinition);
    } else {
      return new BasicElementDefinition<>(childDefinition);
    }
  }

  @Nonnull
  static Optional<FHIRDefinedType> getFhirTypeFromElementDefinition(
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    return Optional.ofNullable(elementDefinition.newInstance())
        .flatMap(ElementDefinition::getFhirTypeFromObject);
  }

  @Nonnull
  static Optional<FHIRDefinedType> getFhirTypeFromObject(@Nonnull final IBase hapiObject) {
    // BackboneElements do not seem to correctly report their FHIR type.
    if (hapiObject.getClass().getSuperclass() == BackboneElement.class) {
      return Optional.of(FHIRDefinedType.BACKBONEELEMENT);
    } else {
      final FHIRDefinedType fhirType = FHIRDefinedType.fromCode(hapiObject.fhirType());
      return Optional.ofNullable(fhirType);
    }
  }

  /**
   * @return the name of this element
   */
  @Nonnull
  String getElementName();

  /**
   * @return The maximum cardinality for this element
   */
  Optional<Integer> getMaxCardinality();

  /**
   * @return The {@link FHIRDefinedType} that corresponds to the type of this element. Not all
   * elements have a type, e.g. polymorphic elements.
   */
  @Nonnull
  Optional<FHIRDefinedType> getFhirType();

}

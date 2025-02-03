package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.model.api.annotation.Block;
import ca.uhn.fhir.model.api.annotation.DatatypeDef;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Encapsulates the FHIR definitions for a child resolved to specific element.
 */
public interface ElementDefinition extends ChildDefinition {

  @Nonnull
  static Optional<FHIRDefinedType> getFhirTypeFromElementDefinition(
      @Nonnull final BaseRuntimeElementDefinition<?> elementDefinition) {
    final Class<?> elementClass = elementDefinition.getImplementingClass();
    return Optional.ofNullable(elementClass.getAnnotation(DatatypeDef.class))
        .map(DatatypeDef::name)
        .map(FHIRDefinedType::fromCode)
        // special case for backbone elements
        .or(() -> Optional.ofNullable(elementClass.getAnnotation(Block.class))
            .map(__ -> FHIRDefinedType.BACKBONEELEMENT));
  }

  /**
   * @return the name of this element
   */
  @Nonnull
  String getElementName();

  /**
   * @return The {@link FHIRDefinedType} that corresponds to the type of this element. Not all
   * elements have a type, e.g. polymorphic elements.
   */
  @Nonnull
  Optional<FHIRDefinedType> getFhirType();
}

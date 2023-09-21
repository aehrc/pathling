package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.*;

import java.util.Optional;
import javax.annotation.Nonnull;

import ca.uhn.fhir.model.api.annotation.DatatypeDef;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Encapsulates the FHIR definitions for a child resolved to specific element.
 */
public interface ElementDefinition extends ChildDefinition {

  @Nonnull
  static Optional<FHIRDefinedType> getFhirTypeFromElementDefinition(
      @Nonnull final BaseRuntimeElementDefinition<?> elementDefinition) {
    return Optional.ofNullable(
            elementDefinition.getImplementingClass().getAnnotation(DatatypeDef.class))
        .map(DatatypeDef::name)
        .map(FHIRDefinedType::fromCode);
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

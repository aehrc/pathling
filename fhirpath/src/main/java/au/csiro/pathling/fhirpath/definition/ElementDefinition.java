package au.csiro.pathling.fhirpath.definition;

import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.DatePath;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.ExtensionPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.element.QuantityPath;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.element.TimePath;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildAny;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.BackboneElement;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public interface ElementDefinition {

  // See https://hl7.org/fhir/fhirpath.html#types.
  @Nonnull
  Map<FHIRDefinedType,
      Class<? extends ElementPath>> FHIR_TYPE_TO_ELEMENT_PATH_CLASS =
      new ImmutableMap.Builder<FHIRDefinedType, Class<? extends ElementPath>>()
          .put(FHIRDefinedType.BOOLEAN, BooleanPath.class)
          .put(FHIRDefinedType.STRING, StringPath.class)
          .put(FHIRDefinedType.URI, StringPath.class)
          .put(FHIRDefinedType.URL, StringPath.class)
          .put(FHIRDefinedType.CANONICAL, StringPath.class)
          .put(FHIRDefinedType.CODE, StringPath.class)
          .put(FHIRDefinedType.OID, StringPath.class)
          .put(FHIRDefinedType.ID, StringPath.class)
          .put(FHIRDefinedType.UUID, StringPath.class)
          .put(FHIRDefinedType.MARKDOWN, StringPath.class)
          .put(FHIRDefinedType.BASE64BINARY, StringPath.class)
          .put(FHIRDefinedType.INTEGER, IntegerPath.class)
          .put(FHIRDefinedType.UNSIGNEDINT, IntegerPath.class)
          .put(FHIRDefinedType.POSITIVEINT, IntegerPath.class)
          .put(FHIRDefinedType.DECIMAL, DecimalPath.class)
          .put(FHIRDefinedType.DATE, DatePath.class)
          .put(FHIRDefinedType.DATETIME, DateTimePath.class)
          .put(FHIRDefinedType.INSTANT, DateTimePath.class)
          .put(FHIRDefinedType.TIME, TimePath.class)
          .put(FHIRDefinedType.CODING, CodingPath.class)
          .put(FHIRDefinedType.QUANTITY, QuantityPath.class)
          .put(FHIRDefinedType.SIMPLEQUANTITY, QuantityPath.class)
          .put(FHIRDefinedType.REFERENCE, ReferencePath.class)
          .put(FHIRDefinedType.EXTENSION, ExtensionPath.class)
          .build();

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

  /**
   * @param fhirType A {@link FHIRDefinedType}
   * @return The subtype of {@link ElementPath} that represents this type
   */
  @Nonnull
  static Optional<Class<? extends ElementPath>> elementClassForType(
      @Nonnull final FHIRDefinedType fhirType) {
    return Optional.ofNullable(
        BasicElementDefinition.FHIR_TYPE_TO_ELEMENT_PATH_CLASS.get(fhirType));
  }

  @Nonnull
  static Optional<FHIRDefinedType> getFhirTypeFromElementDefinition(
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    return Optional.ofNullable(elementDefinition.newInstance())
        .flatMap(ElementDefinition::getFhirTypeFromObject);
  }

  static Optional<FHIRDefinedType> getFhirTypeFromObject(final IBase hapiObject) {
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
   * Returns the child element of this element with the specified name.
   *
   * @param name The name of the child element
   * @return A new ElementDefinition describing the child
   */
  @Nonnull
  Optional<ElementDefinition> getChildElement(@Nonnull String name);

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

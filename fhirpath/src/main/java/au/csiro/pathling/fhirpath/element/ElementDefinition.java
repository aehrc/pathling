/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.element;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildAny;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.BackboneElement;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Encapsulates the FHIR definitions for an element.
 *
 * @author John Grimes
 */
public class ElementDefinition {

  // See https://hl7.org/fhir/fhirpath.html#types.
  @Nonnull
  private static final Map<FHIRDefinedType,
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

  @Nonnull
  private final BaseRuntimeChildDefinition childDefinition;

  @Nonnull
  private final Optional<BaseRuntimeElementDefinition> elementDefinition;

  protected ElementDefinition(@Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {
    this.childDefinition = childDefinition;
    elementDefinition = Optional.ofNullable(childDefinition.getChildByName(elementName));
  }

  /**
   * @param childDefinition A HAPI {@link BaseRuntimeChildDefinition} that describes this element
   * @param elementName The name of the element
   * @return A shiny new ElementDefinition
   */
  @Nonnull
  public static ElementDefinition build(@Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {
    if (elementName.equals("valueReference") && childDefinition instanceof RuntimeChildAny) {
      return new ReferenceExtensionDefinition(childDefinition, elementName);
    } else if (childDefinition instanceof RuntimeChildResourceDefinition) {
      return new ReferenceDefinition((RuntimeChildResourceDefinition) childDefinition, elementName);
    } else {
      return new ElementDefinition(childDefinition, elementName);
    }
  }

  /**
   * Returns the child element of this element with the specified name.
   *
   * @param name The name of the child element
   * @return A new ElementDefinition describing the child
   */
  @Nonnull
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    if (elementDefinition.isPresent()
        && elementDefinition.get() instanceof BaseRuntimeElementCompositeDefinition) {
      final BaseRuntimeElementCompositeDefinition compositeDefinition =
          (BaseRuntimeElementCompositeDefinition) elementDefinition.get();
      final BaseRuntimeChildDefinition newChild = compositeDefinition.getChildByName(name);
      if (newChild == null) {
        return Optional.empty();
      }
      return Optional.of(ElementDefinition.build(newChild, name));
    } else {
      return Optional.empty();
    }
  }

  /**
   * @return The maximum cardinality for this element
   */
  public int getMaxCardinality() {
    return childDefinition.getMax();
  }

  /**
   * Returns the set of resources that a reference can refer to.
   *
   * @return A set of {@link ResourceType} objects, if this element is a reference
   */
  @Nonnull
  public Set<ResourceType> getReferenceTypes() {
    return Collections.emptySet();
  }

  /**
   * @return The {@link FHIRDefinedType} that corresponds to the type of this element. Not all
   * elements have a type, e.g. polymorphic elements.
   */
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    if (elementDefinition.isEmpty()) {
      return Optional.empty();
    }

    @Nullable final IBase exampleObject = elementDefinition.get().newInstance();
    if (exampleObject == null || exampleObject.getClass() == null) {
      return Optional.empty();
    }

    return getFhirTypeFromObject(exampleObject);
  }

  /**
   * @param fhirType A {@link FHIRDefinedType}
   * @return The subtype of {@link ElementPath} that represents this type
   */
  @Nonnull
  public static Optional<Class<? extends ElementPath>> elementClassForType(
      @Nonnull final FHIRDefinedType fhirType) {
    return Optional.ofNullable(FHIR_TYPE_TO_ELEMENT_PATH_CLASS.get(fhirType));
  }

  private static Optional<FHIRDefinedType> getFhirTypeFromObject(final IBase hapiObject) {
    // BackboneElements do not seem to correctly report their FHIR type.
    if (hapiObject.getClass().getSuperclass() == BackboneElement.class) {
      return Optional.of(FHIRDefinedType.BACKBONEELEMENT);
    } else {
      final FHIRDefinedType fhirType = FHIRDefinedType.fromCode(hapiObject.fhirType());
      return Optional.ofNullable(fhirType);
    }
  }

}

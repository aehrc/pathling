/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.definition;

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.ChoiceDefinition;
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.definition.NodeDefinition;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.schema.PrimitiveTypes;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The definitions under which a primitive's id and extensions are read (decision 141, D14).
 *
 * <p>The definition of every resource the engine resolves is wrapped (decision 154), so that each
 * definition reached from it is wrapped too. A wrapped primitive is held as one structure of its
 * value, its {@code id} and {@code extension}, and its annotations (decision 152), and has the
 * children {@code id} and {@code extension}, which the definitions of FHIR primitives do not
 * declare. Traversal and the primitive collections recognise the wrapped definition, so it travels
 * with a collection through every operation that keeps its definition. A definition that is not
 * wrapped, such as one an arbitrary object is resolved under, gives a primitive no children, so
 * that navigating to them is empty.
 *
 * <p>The {@code id} of a primitive is a plain string, not itself a primitive with an id and
 * extensions, so its definition is not wrapped.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class PrimitiveMetadata {

  /** The name of a primitive's id in its structure and in its metadata group. */
  @Nonnull public static final String ID = "id";

  /** The name of a primitive's extensions in its structure and in its metadata group. */
  @Nonnull public static final String EXTENSION = "extension";

  /** The definition of a primitive's id. */
  @Nonnull private final ElementDefinition idDefinition;

  /** The definition of a primitive's extensions, not wrapped. */
  @Nonnull private final ElementDefinition extensionDefinition;

  /**
   * Creates the definitions of a primitive's id and extensions from a definition context.
   *
   * @param definitionContext the context the definitions are found in
   * @return the definitions
   */
  @Nonnull
  public static PrimitiveMetadata of(@Nonnull final FhirDefinitionContext definitionContext) {
    return new PrimitiveMetadata(
        definitionContext.findElementChild(ID), definitionContext.findElementChild(EXTENSION));
  }

  /**
   * Returns a resource collection whose definition, and every definition reached from it, reads a
   * primitive's id and extensions.
   *
   * @param resource the resource collection
   * @return the resource collection, under the wrapped definition
   */
  @Nonnull
  public ResourceCollection apply(@Nonnull final ResourceCollection resource) {
    return resource.withResourceDefinition(
        new MetadataResourceDefinition(resource.getResourceDefinition(), this));
  }

  /**
   * Wraps a child definition, so that it and every definition reached from it reads a primitive's
   * id and extensions.
   *
   * @param child the child definition
   * @return the wrapped definition
   */
  @Nonnull
  ChildDefinition wrapChild(@Nonnull final ChildDefinition child) {
    return switch (child) {
      case final ElementDefinition element -> wrapElement(element);
      case final ChoiceDefinition choice -> new MetadataChoiceDefinition(choice, this);
      default -> child;
    };
  }

  /**
   * Wraps an element definition, so that it and every definition reached from it reads a
   * primitive's id and extensions.
   *
   * @param element the element definition
   * @return the wrapped definition
   */
  @Nonnull
  ElementDefinition wrapElement(@Nonnull final ElementDefinition element) {
    return new MetadataElementDefinition(element, this);
  }

  /**
   * Returns whether a definition is that of a primitive held with its id and extensions, so that
   * its values are structures of the value, the id, the extensions and the annotations.
   *
   * @param definition the definition, where there is one
   * @return true where the definition is of a primitive that carries its id and extensions
   */
  public static boolean carriedBy(@Nonnull final Optional<? extends NodeDefinition> definition) {
    return definition
        .filter(MetadataElementDefinition.class::isInstance)
        .map(MetadataElementDefinition.class::cast)
        .filter(MetadataElementDefinition::isPrimitive)
        .isPresent();
  }

  /**
   * Returns whether a FHIR type is a primitive that has an id and extensions. The XHTML of a
   * narrative is left out, as it has no FHIRPath type and no metadata group.
   *
   * @param fhirType the FHIR type
   * @return true where the type is such a primitive
   */
  static boolean isPrimitive(@Nonnull final Optional<FHIRDefinedType> fhirType) {
    return fhirType
        .filter(type -> type != FHIRDefinedType.XHTML && PrimitiveTypes.isPrimitive(type))
        .isPresent();
  }
}

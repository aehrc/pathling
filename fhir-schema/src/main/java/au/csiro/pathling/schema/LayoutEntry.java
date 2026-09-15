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

package au.csiro.pathling.schema;

import au.csiro.pathling.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * One field of a stored structure, as the canonical order declares it.
 *
 * <p>A field is one of four things: the resource type, an element the definitions describe, the
 * metadata group beside a primitive, or an annotation. Every one of them but the resource type
 * knows the element it is or accompanies, so that a caller can take cardinality and type from the
 * definitions rather than from the name.
 */
public final class LayoutEntry {

  private static final String RESOURCE_TYPE = "resourceType";
  private static final String ELEMENT = "element";
  private static final String METADATA_GROUP = "metadataGroup";
  private static final String ANNOTATION = "annotation";

  @Nonnull private final String name;

  @Nonnull private final String kind;

  @Nonnull private final Optional<ElementDefinition> element;

  private final boolean fromOpenChoice;

  private LayoutEntry(
      @Nonnull final String name,
      @Nonnull final String kind,
      @Nonnull final Optional<ElementDefinition> element,
      final boolean fromOpenChoice) {
    this.name = name;
    this.kind = kind;
    this.element = element;
    this.fromOpenChoice = fromOpenChoice;
  }

  /**
   * Returns the entry for the field naming the type of a resource.
   *
   * @return the entry
   */
  @Nonnull
  public static LayoutEntry resourceType() {
    return new LayoutEntry(LayoutFields.RESOURCE_TYPE, RESOURCE_TYPE, Optional.empty(), false);
  }

  /**
   * Returns the entry for an element the definitions describe.
   *
   * @param name the name of the field, which is the qualified name where the element is a variant
   *     of a choice
   * @param element the definition of the element
   * @param fromOpenChoice whether the element is a variant of an open choice
   * @return the entry
   */
  @Nonnull
  public static LayoutEntry element(
      @Nonnull final String name,
      @Nonnull final ElementDefinition element,
      final boolean fromOpenChoice) {
    return new LayoutEntry(name, ELEMENT, Optional.of(element), fromOpenChoice);
  }

  /**
   * Returns the entry for the metadata group beside a primitive.
   *
   * @param name the name of the group
   * @param element the definition of the element it accompanies
   * @param fromOpenChoice whether that element is a variant of an open choice
   * @return the entry
   */
  @Nonnull
  public static LayoutEntry metadataGroup(
      @Nonnull final String name,
      @Nonnull final ElementDefinition element,
      final boolean fromOpenChoice) {
    return new LayoutEntry(name, METADATA_GROUP, Optional.of(element), fromOpenChoice);
  }

  /**
   * Returns the entry for an annotation.
   *
   * @param name the name of the annotation
   * @param element the definition of the element it annotates
   * @param fromOpenChoice whether that element is a variant of an open choice
   * @return the entry
   */
  @Nonnull
  public static LayoutEntry annotation(
      @Nonnull final String name,
      @Nonnull final ElementDefinition element,
      final boolean fromOpenChoice) {
    return new LayoutEntry(name, ANNOTATION, Optional.of(element), fromOpenChoice);
  }

  /**
   * Returns the name of this field.
   *
   * @return the field name
   */
  @Nonnull
  public String getName() {
    return name;
  }

  /**
   * Returns the definition of the element this field is or accompanies.
   *
   * @return the element definition, or empty for the resource type
   */
  @Nonnull
  public Optional<ElementDefinition> getElement() {
    return element;
  }

  /**
   * Returns whether the element this field is or accompanies is a variant of an open choice, whose
   * expansion is bounded by configuration.
   *
   * @return true where the element is a variant of an open choice
   */
  public boolean isFromOpenChoice() {
    return fromOpenChoice;
  }

  /**
   * Returns whether this field names the type of a resource.
   *
   * @return true where it does
   */
  public boolean isResourceType() {
    return RESOURCE_TYPE.equals(kind);
  }

  /**
   * Returns whether this field is an element the definitions describe.
   *
   * @return true where it is
   */
  public boolean isElement() {
    return ELEMENT.equals(kind);
  }

  /**
   * Returns whether this field is the metadata group beside a primitive.
   *
   * @return true where it is
   */
  public boolean isMetadataGroup() {
    return METADATA_GROUP.equals(kind);
  }

  /**
   * Returns whether this field is an annotation.
   *
   * @return true where it is
   */
  public boolean isAnnotation() {
    return ANNOTATION.equals(kind);
  }

  @Override
  @Nonnull
  public String toString() {
    return kind + "(" + name + ")";
  }
}

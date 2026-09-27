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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.encoders.ExtensionSupport;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The definition of an element that the engine builds itself, such as a Coding returned by a
 * terminology operation, which offers every child of the element's definition except its
 * extensions.
 *
 * <p>A structure the engine builds carries a null field identifier, so that it has the type the
 * previous layout gives the element. Extension traversal over it would look that identifier up in
 * the previous layout's extension map, which a new-layout table does not have. The structure has no
 * extensions on either layout, so its definition does not offer them, and traversal to them is
 * empty. Before T094a this followed from the structure carrying no extension map.
 *
 * @author Piotr Szul
 */
@ToString
@EqualsAndHashCode
public class WithoutExtensionsDefinition implements ElementDefinition {

  @Nonnull private final ElementDefinition definition;

  /**
   * Creates the definition of an element the engine builds, from the definition of the element.
   *
   * @param definition the definition of the element
   */
  public WithoutExtensionsDefinition(@Nonnull final ElementDefinition definition) {
    this.definition = definition;
  }

  @Override
  @Nonnull
  public String getName() {
    return definition.getName();
  }

  @Override
  @Nonnull
  public String getElementName() {
    return definition.getElementName();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return definition.getFhirType();
  }

  @Override
  public int getMaxCardinality() {
    return definition.getMaxCardinality();
  }

  @Override
  @Nonnull
  public List<ChildDefinition> getChildren() {
    return definition.getChildren().stream()
        .filter(child -> !isExtension(child.getName()))
        .toList();
  }

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return isExtension(name) ? Optional.empty() : definition.getChildElement(name);
  }

  @Override
  public boolean isRepeating() {
    return definition.isRepeating();
  }

  @Override
  public boolean isChoiceElement() {
    return definition.isChoiceElement();
  }

  @Override
  @Nonnull
  public Object getTypeIdentity() {
    return definition.getTypeIdentity();
  }

  @Override
  public boolean isFhirDefinition() {
    return definition.isFhirDefinition();
  }

  private static boolean isExtension(@Nonnull final String name) {
    return ExtensionSupport.EXTENSION_ELEMENT_NAME().equals(name);
  }
}

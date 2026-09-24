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

package au.csiro.pathling.definition.fhir;

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.ChoiceDefinition;
import au.csiro.pathling.definition.ElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Represents the definition of an element that can be represented by multiple different data types.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#polymorphism">Polymorphism in FHIR</a>
 */
class FhirChoiceDefinition implements ChoiceDefinition {

  /** The type every resource a choice can target is carried as. */
  @Nonnull private static final String REFERENCE_TYPE = "Reference";

  @Nonnull private final RuntimeChildChoiceDefinition childDefinition;

  protected FhirChoiceDefinition(@Nonnull final RuntimeChildChoiceDefinition childDefinition) {
    this.childDefinition = childDefinition;
  }

  @Nonnull
  @Override
  public String getName() {
    return childDefinition.getElementName();
  }

  @Nonnull
  @Override
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return getChildByElementName(name).map(e -> e);
  }

  @Nonnull
  @Override
  public List<ChildDefinition> getChildren() {
    // The children of a choice are the types it can take.
    return getAllChildTypes().stream().map(ChildDefinition.class::cast).toList();
  }

  /**
   * Returns the child element definition for the given type, if it exists.
   *
   * @param type the type of the child element
   * @return the child element definition, if it exists
   */
  @Nonnull
  public Optional<ElementDefinition> getChildByType(@Nonnull final String type) {
    final String key = ChoiceDefinition.columnName(getName(), type);
    return getChildByElementName(key);
  }

  @Nonnull
  @Override
  public List<ElementDefinition> getAllChildTypes() {
    // The order comes from the declared list of types, which is the type list of the child
    // annotation and so is declaration order verbatim. It deliberately does not come from the set
    // of valid child names, which is hash ordered: that order is neither reproducible by another
    // implementation nor stable across an upgrade of the definition library, and the order of an
    // expansion is part of the type of every structure that carries the choice.
    return childDefinition.getChoices().stream()
        .map(this::nameOfDeclaredType)
        .distinct()
        .flatMap(name -> getChildByElementName(name).stream())
        .toList();
  }

  /**
   * Returns the name a declared type takes within this choice.
   *
   * <p>A type the choice can reference is declared as the resource it targets, and every such
   * target is carried by the same reference. They therefore share one name, which {@link
   * #getAllChildTypes()} reduces to a single variant at the position of the first target declared.
   * The definition library also accepts an alias per target and an alias for an untyped resource,
   * but no FHIR instance can populate one, so naming them would put columns into every stored
   * structure that carries the choice which nothing could ever fill.
   *
   * <p>A type that is not a reference target takes the name the definition library gives it. A type
   * the library maps to no name is neither, and so is an inconsistency in the definitions rather
   * than a name to be invented: inventing one would produce a variant that resolves to nothing, and
   * the mistake would be invisible.
   */
  @Nonnull
  private String nameOfDeclaredType(@Nonnull final Class<? extends IBase> type) {
    if (childDefinition.getResourceTypes().contains(type)) {
      return ChoiceDefinition.columnName(getName(), REFERENCE_TYPE);
    }
    return Optional.ofNullable(childDefinition.getChildNameByDatatype(type))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "The definitions give the type "
                        + type.getName()
                        + " no name within the choice "
                        + getName()));
  }

  /**
   * Returns the child element definition for the given element name, if it exists.
   *
   * @param name the name of the child element
   * @return the child element definition, if it exists
   */
  @Nonnull
  private Optional<ElementDefinition> getChildByElementName(final String name) {
    return FhirDefinitionContext.buildElement(childDefinition, name);
  }
}

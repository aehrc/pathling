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
import ca.uhn.fhir.context.RuntimeChildAny;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Represents the definition of an element that can be represented by multiple different data types.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#polymorphism">Polymorphism in FHIR</a>
 */
class FhirChoiceDefinition implements ChoiceDefinition {

  @Nonnull private final RuntimeChildChoiceDefinition childDefinition;

  protected FhirChoiceDefinition(@Nonnull final RuntimeChildChoiceDefinition childDefinition) {
    this.childDefinition = childDefinition;
  }

  /**
   * Returns the column name for a given type.
   *
   * @param elementName the name of the parent element
   * @param type the type of the child element
   * @return the column name
   */
  @Nonnull
  public static String getColumnName(
      @Nonnull final String elementName, @Nonnull final String type) {
    return elementName + StringUtils.capitalize(type);
  }

  @Nonnull
  @Override
  public String getName() {
    return childDefinition.getElementName();
  }

  @Override
  public boolean isOpenType() {
    return childDefinition instanceof RuntimeChildAny;
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
    final String key = FhirChoiceDefinition.getColumnName(getName(), type);
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
    final Set<String> valid = new LinkedHashSet<>(childDefinition.getValidChildNames());
    final List<String> declared =
        childDefinition.getChoices().stream()
            .map(this::nameOfDeclaredType)
            .filter(valid::contains)
            .distinct()
            .toList();
    // A choice that admits a reference carries names the declared types do not account for: the
    // plain reference and the untyped resource. They have no declared position, so they follow in
    // a stated order rather than in the order the name set happens to iterate in.
    final Stream<String> remainder =
        valid.stream().filter(name -> !declared.contains(name)).sorted();
    return Stream.concat(declared.stream(), remainder)
        .flatMap(name -> getChildByElementName(name).stream())
        .toList();
  }

  /**
   * Returns the name a declared type takes within this choice. A type that admits a reference is
   * declared as the resource it targets, which the definition library does not map to a name, so
   * the name is built the way the library builds it.
   */
  @Nonnull
  private String nameOfDeclaredType(@Nonnull final Class<? extends IBase> type) {
    return Optional.ofNullable(childDefinition.getChildNameByDatatype(type))
        .orElseGet(() -> getColumnName(getName(), type.getSimpleName()));
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

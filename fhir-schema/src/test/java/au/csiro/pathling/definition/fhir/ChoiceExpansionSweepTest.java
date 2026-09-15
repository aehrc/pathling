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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.ElementDefinition;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Extension;
import org.junit.jupiter.api.Test;

/**
 * Pins the expansion of every choice in every R4 resource against the names the definition library
 * says are valid for it.
 *
 * <p>The order of an expansion is taken from the declared type list rather than from the set of
 * valid names, because the set is hash ordered. This sweep is what says the two describe the same
 * variants: a declared type that maps to no name, or to a name outside the valid set, would
 * silently drop a variant from every stored structure that carries the choice.
 */
class ChoiceExpansionSweepTest {

  @Nonnull private static final FhirContext CONTEXT = FhirContext.forR4();

  @Test
  void expandsEveryChoiceToExactlyTheVariantsTheDefinitionsValidate() {
    final Set<BaseRuntimeElementDefinition<?>> visited = new HashSet<>();
    final List<String> checked = new ArrayList<>();
    for (final ResourceType type : ResourceType.values()) {
      resourceDefinition(type).ifPresent(definition -> visit(definition, visited, checked));
    }
    // A sweep that visited nothing would pass every assertion within it.
    assertTrue(
        checked.size() > 100,
        "Expected the sweep to reach a substantial number of choices, but it reached "
            + checked.size());
  }

  @Nonnull
  private static Optional<RuntimeResourceDefinition> resourceDefinition(
      @Nonnull final ResourceType type) {
    if (ResourceType.NULL.equals(type)) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(CONTEXT.getResourceDefinition(type.toCode()));
    } catch (final RuntimeException e) {
      // A type the definition library does not carry a structure for.
      return Optional.empty();
    }
  }

  private static void visit(
      @jakarta.annotation.Nullable final BaseRuntimeElementDefinition<?> definition,
      @Nonnull final Set<BaseRuntimeElementDefinition<?>> visited,
      @Nonnull final List<String> checked) {
    if (!(definition instanceof final BaseRuntimeElementCompositeDefinition<?> composite)
        || !visited.add(definition)) {
      return;
    }
    for (final BaseRuntimeChildDefinition child : composite.getChildren()) {
      if (child instanceof final RuntimeChildChoiceDefinition choice
          && !(child instanceof RuntimeChildExtension)) {
        check(choice, checked);
      }
      if (child instanceof RuntimeChildExtension) {
        // An extension child cannot be resolved by name, because HAPI resolves every extension
        // child against the name of the plain extension element. The type is taken directly, so
        // that the open choice within an extension is still reached.
        visit(child.getChildElementDefinitionByDatatype(Extension.class), visited, checked);
        continue;
      }
      for (final String name : child.getValidChildNames()) {
        final BaseRuntimeElementDefinition<?> childElement = child.getChildByName(name);
        if (childElement != null) {
          visit(childElement, visited, checked);
        }
      }
    }
  }

  private static void check(
      @Nonnull final RuntimeChildChoiceDefinition choice, @Nonnull final List<String> checked) {
    final String path = choice.getElementName();
    final List<String> expanded =
        new FhirChoiceDefinition(choice)
            .getAllChildTypes().stream().map(ElementDefinition::getElementName).toList();

    assertEquals(
        expanded.size(),
        Set.copyOf(expanded).size(),
        "Duplicate variant in the expansion of " + path);
    assertEquals(
        Set.copyOf(choice.getValidChildNames()),
        Set.copyOf(expanded),
        "The expansion of " + path + " does not describe the variants the definitions validate");
    checked.add(path);
  }
}

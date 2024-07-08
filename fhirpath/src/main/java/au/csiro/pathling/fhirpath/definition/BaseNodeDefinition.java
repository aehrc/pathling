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

package au.csiro.pathling.fhirpath.definition;

import static au.csiro.pathling.utilities.Functions.maybeCast;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Common behaviour for all node definitions.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
abstract public class BaseNodeDefinition<ED extends BaseRuntimeElementDefinition<?>> implements
    NodeDefinition {

  @Nonnull
  protected final ED elementDefinition;

  @Nonnull
  final Map<String, RuntimeChildChoiceDefinition> nestedChildElementsByName;

  protected BaseNodeDefinition(@Nonnull final ED elementDefinition) {
    this.elementDefinition = elementDefinition;

    // Create a stream of all the definitions of the children of this element.
    //noinspection unchecked
    final Stream<BaseRuntimeChildDefinition> allChildren = Optional.of(elementDefinition)
        // Cast to composite element definition, and if it is one then get its children.
        .flatMap(maybeCast(BaseRuntimeElementCompositeDefinition.class)).stream()
        .flatMap(compElementDef -> compElementDef.getChildren().stream());

    // Create a map of all the qualified choice children by name. This is used to resolve the 
    // correct child definition when a qualified choice element is traversed.
    nestedChildElementsByName = allChildren
        // Filter out non-choice children, then cast to choice definitions.
        .filter(ChildDefinition::isChildChoiceDefinition)
        .map(RuntimeChildChoiceDefinition.class::cast)
        .flatMap(
            // Create a stream of pairs of child names and their respective choice definitions.
            choiceDef -> choiceDef.getValidChildNames().stream().map(n -> Pair.of(n, choiceDef)))
        // Collect the pairs into a map.
        .collect(Collectors.toUnmodifiableMap(Pair::getLeft, Pair::getRight));
  }

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    // If the child is a qualified choice element (e.g. valueString), resolve the correct child 
    // definition using the pre-built map.
    final RuntimeChildChoiceDefinition choiceChild = nestedChildElementsByName.get(name);
    if (choiceChild != null) {
      return Optional.of(new ElementChildDefinition(choiceChild, name));
    }

    // If the child is not a qualified choice, look for the child definition by name.
    return Optional.of(elementDefinition)
        // All traversable elements are composite.
        .flatMap(maybeCast(BaseRuntimeElementCompositeDefinition.class))
        // Get the child definition by name...
        .flatMap(compElementDef -> Optional.ofNullable(compElementDef.getChildByName(name))
            // Or by name with a suffix "[x]" in the case of an unqualified choice (e.g. value[x]).
            .or(() -> Optional.ofNullable(compElementDef.getChildByName(name + "[x]"))))
        // The ChildDefinition.build method is a factory method that creates the correct
        // implementation of ChildDefinition based on the type of the child.
        .map(ChildDefinition::build);
  }

  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return ElementDefinition.getFhirTypeFromElementDefinition(elementDefinition);
  }

}

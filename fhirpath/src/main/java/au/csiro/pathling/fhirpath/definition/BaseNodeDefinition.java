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

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

import static au.csiro.pathling.utilities.Functions.maybeCast;

/**
 * Base implemention of a NodeDefition based on a BaseRuntimeElementDefinition.
 *
 * @author John Grimes
 */
abstract public class BaseNodeDefinition<ED extends BaseRuntimeElementDefinition<?>> implements
    NodeDefinition {

  @Nonnull
  protected final ED elementDefinition;

  @Nonnull
  final Map<String, RuntimeChildChoiceDefinition> nestedChildElementsByName;

  protected BaseNodeDefinition(@Nonnull final ED elementDefinition) {
    this.elementDefinition = elementDefinition;

    // we need to map all choices to be available as children here
    //noinspection unchecked
    final Stream<BaseRuntimeChildDefinition> allChildren = Optional.of(elementDefinition)
        .flatMap(maybeCast(BaseRuntimeElementCompositeDefinition.class)).stream()
        .flatMap(compElementDef -> compElementDef.getChildren().stream());

    nestedChildElementsByName = allChildren
        .filter(ChildDefinition::isChildChoiceDefinition)
        .map(RuntimeChildChoiceDefinition.class::cast)
        .flatMap(
            choiceDef -> choiceDef.getValidChildNames().stream().map(n -> Pair.of(n, choiceDef)))
        .collect(Collectors.toUnmodifiableMap(Pair::getLeft, Pair::getRight));
  }

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {

    // TODO: make it nicer
    final RuntimeChildChoiceDefinition choiceChild = nestedChildElementsByName.get(name);
    if (choiceChild != null) {
      return Optional.of(new ElementChildDefinition(choiceChild, name));
    }

    return Optional.of(elementDefinition)
        .flatMap(maybeCast(BaseRuntimeElementCompositeDefinition.class))
        .flatMap(compElementDef -> Optional.ofNullable(compElementDef.getChildByName(name))
            .or(() -> Optional.ofNullable(compElementDef.getChildByName(name + "[x]"))))
        .map(ChildDefinition::build);
  }

  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return ElementDefinition.getFhirTypeFromElementDefinition(elementDefinition);
  }

}

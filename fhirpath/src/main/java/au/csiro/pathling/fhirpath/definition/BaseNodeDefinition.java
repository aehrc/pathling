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

import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Base implemention of a NodeDefition based on a BaseRuntimeElementDefinition.
 *
 * @author John Grimes
 */
abstract public class BaseNodeDefinition<ED extends BaseRuntimeElementDefinition<?>> implements
    NodeDefinition {

  @Nonnull
  protected final ED elementDefinition;

  protected BaseNodeDefinition(@Nonnull final ED elementDefinition) {
    this.elementDefinition = elementDefinition;
  }

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return Optional.of(elementDefinition)
        .filter(BaseRuntimeElementCompositeDefinition.class::isInstance)
        .map(BaseRuntimeElementCompositeDefinition.class::cast)
        .flatMap(compElementDef -> Optional.ofNullable(compElementDef.getChildByName(name))
            .or(() -> Optional.ofNullable(compElementDef.getChildByName(name + "[x]"))))
        .map(ChildDefinition::build);
  }

  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return ElementDefinition.getFhirTypeFromElementDefinition(elementDefinition);
  }

}

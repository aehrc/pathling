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
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Encapsulates the FHIR definitions for an element.
 *
 * @author John Grimes
 */
public class BasicElementDefinition<ChildDefinitionType extends BaseRuntimeChildDefinition> implements
    ElementDefinition {

  @Nonnull
  protected final ChildDefinitionType childDefinition;

  @Nonnull
  protected final Optional<BaseRuntimeElementDefinition> elementDefinition;

  protected BasicElementDefinition(@Nonnull final ChildDefinitionType childDefinition) {
    this.childDefinition = childDefinition;
    elementDefinition = getElementDefinition();
  }

  @Nonnull
  protected Optional<BaseRuntimeElementDefinition> getElementDefinition() {
    @Nullable BaseRuntimeElementDefinition<?> child;
    try {
      child = childDefinition.getChildByName(childDefinition.getElementName());
    } catch (final Throwable e) {
      child = null;
    }
    return Optional.ofNullable(child);
  }

  @Override
  @Nonnull
  public String getElementName() {
    return childDefinition.getElementName();
  }

  @Override
  @Nonnull
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return elementDefinition
        .filter(elementDef -> elementDef instanceof BaseRuntimeElementCompositeDefinition)
        .map(compElementDef -> (BaseRuntimeElementCompositeDefinition) compElementDef)
        .flatMap(compElementDef -> Optional.ofNullable(compElementDef.getChildByName(name))
            .or(() -> Optional.ofNullable(compElementDef.getChildByName(name + "[x]"))))
        .map(ElementDefinition::build);
  }

  @Override
  public Optional<Integer> getMaxCardinality() {
    return Optional.of(childDefinition.getMax());
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return elementDefinition.flatMap(ElementDefinition::getFhirTypeFromElementDefinition);
  }

}

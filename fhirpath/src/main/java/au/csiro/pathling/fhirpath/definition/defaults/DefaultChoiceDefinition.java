/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.definition.defaults;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

/**
 * The default implementation of a choice data type allowing for explicit definition of its possible
 * choices and other properties. This class represents FHIR choice elements, which can contain one
 * of several possible types (e.g., valueString, valueInteger, etc.).
 */
@Value(staticConstructor = "of")
public class DefaultChoiceDefinition implements ChoiceDefinition {

  /**
   * The base name of this choice element, without any type suffix.
   */
  @Nonnull
  String name;

  /**
   * The list of possible child definitions that this choice element can contain. Each child
   * represents a different possible type for this choice element.
   */
  List<ChildDefinition> choices;

  /**
   * Returns the child element definition for the given type, if it exists. For example, if the
   * choice element is named "value" and the type is "string", this method will look for a child
   * named "valueString".
   *
   * @param type the type of the child element (e.g., "string", "integer")
   * @return the child element definition, if it exists
   */
  @Override
  @Nonnull
  public Optional<ElementDefinition> getChildByType(@Nonnull final String type) {
    return getChildElement(name + StringUtils.capitalize(type))
        .map(ElementDefinition.class::cast);
  }

  /**
   * Returns the child element with the specified name, if it exists in the list of choices. This
   * method is used to find a specific type option within this choice element.
   *
   * @param name the name of the child element to find
   * @return the child element, if it exists
   */
  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return choices.stream()
        .filter(child -> child.getName().equals(name))
        .findFirst();
  }
}

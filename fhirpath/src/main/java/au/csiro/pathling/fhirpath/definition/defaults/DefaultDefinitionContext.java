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

package au.csiro.pathling.fhirpath.definition.defaults;

import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * A default implementation of {@link DefinitionContext} that allows for explicit definition of
 * resource types.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DefaultDefinitionContext implements DefinitionContext {

  @Nonnull Map<String, ResourceDefinition> resourceDefinitions;

  @Override
  @Nonnull
  public ResourceDefinition findResourceDefinition(@Nonnull final String resourceType) {
    return Optional.ofNullable(resourceDefinitions.get(resourceType))
        .orElseThrow(
            () -> new IllegalArgumentException("Resource type not found: " + resourceType));
  }

  /**
   * Creates a new DefaultDefinitionContext from the given resource definitions.
   *
   * @param resourceDefinitions the resource definitions to include
   * @return a new DefaultDefinitionContext
   */
  @Nonnull
  public static DefaultDefinitionContext of(final ResourceDefinition... resourceDefinitions) {
    return new DefaultDefinitionContext(
        Stream.of(resourceDefinitions)
            .collect(Collectors.toMap(ResourceDefinition::getResourceCode, Function.identity())));
  }
}

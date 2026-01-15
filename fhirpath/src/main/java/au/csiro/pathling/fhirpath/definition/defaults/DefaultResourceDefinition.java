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

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;

/**
 * The default implementation of {@link ResourceDefinition} allowing for explicit definition of its
 * children.
 */
@Value(staticConstructor = "of")
public class DefaultResourceDefinition implements ResourceDefinition {

  @Nonnull DefaultResourceTag resourceTag;
  @Nonnull List<ChildDefinition> children;

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return children.stream().filter(child -> child.getName().equals(name)).findFirst();
  }

  /**
   * Creates a new DefaultResourceDefinition with the given resource tag and children.
   *
   * @param resourceTag the resource tag
   * @param children the child definitions
   * @return a new DefaultResourceDefinition
   */
  @Nonnull
  public static DefaultResourceDefinition of(
      @Nonnull final DefaultResourceTag resourceTag, final ChildDefinition... children) {
    return DefaultResourceDefinition.of(resourceTag, List.of(children));
  }
}

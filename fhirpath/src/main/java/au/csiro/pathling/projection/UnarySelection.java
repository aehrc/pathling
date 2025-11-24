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

package au.csiro.pathling.projection;

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * Represents a selection that wraps a single child component.
 * <p>
 * This interface provides default implementations for tree traversal methods,
 * allowing concrete implementations to focus on their specific behavior.
 */
public interface UnarySelection extends ProjectionClause {

  /**
   * Returns the single child component of this selection.
   * <p>
   * This method is automatically implemented by record classes with a {@code component} field.
   *
   * @return the child projection clause
   */
  @Nonnull
  ProjectionClause component();

  /**
   * Returns a stream containing the single child component.
   *
   * @return a stream with one element - the component
   */
  @Nonnull
  @Override
  default Stream<ProjectionClause> getChildren() {
    return Stream.of(component());
  }
}

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

package au.csiro.pathling.projection;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * Represents a selection that contains multiple child components.
 *
 * <p>This interface provides default implementations for tree traversal methods, allowing concrete
 * implementations to focus on their specific behavior.
 */
public interface CompositeSelection extends ProjectionClause {

  /**
   * Returns the list of child components of this selection.
   *
   * <p>This method is automatically implemented by record classes with a {@code components} field.
   *
   * @return the list of child projection clauses
   */
  @Nonnull
  List<ProjectionClause> components();

  /**
   * Returns a stream of all child components.
   *
   * @return a stream of the components
   */
  @Nonnull
  @Override
  default Stream<ProjectionClause> getChildren() {
    return components().stream();
  }
}

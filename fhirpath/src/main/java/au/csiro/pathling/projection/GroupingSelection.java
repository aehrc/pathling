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

/**
 * Groups multiple selections together using a cross join (Cartesian product).
 *
 * <p>This is the primary mechanism for composing multiple projection clauses into a single result
 * where all combinations of the component results are included.
 *
 * @param components the list of projection clauses to be combined via product
 * @author John Grimes
 * @author Piotr Szul
 */
public record GroupingSelection(@Nonnull List<ProjectionClause> components)
    implements CompositeSelection {

  @Override
  @Nonnull
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Evaluate all components
    final List<ProjectionResult> subResults =
        components.stream().map(c -> c.evaluate(context)).toList();
    // Use the explicit product method for clarity
    return ProjectionResult.product(subResults);
  }

  /**
   * Returns a string expression representation of this grouping selection.
   *
   * @return the string expression "group"
   */
  @Nonnull
  @Override
  public String toExpression() {
    return "group";
  }
}

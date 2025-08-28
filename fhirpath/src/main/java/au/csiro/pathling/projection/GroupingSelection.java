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

import static java.util.stream.Collectors.joining;

import jakarta.annotation.Nonnull;
import java.util.List;

/**
 * Groups multiple selections together using a cross join.
 *
 * @param components the list of projection clauses to be grouped
 * @author John Grimes
 * @author Piotr Szul
 */
public record GroupingSelection(@Nonnull List<ProjectionClause> components) implements
    ProjectionClause {

  @Override
  @Nonnull
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // evaluate and cross join the subcomponents
    final List<ProjectionResult> subResults = components.stream().map(c -> c.evaluate(context))
        .toList();
    return ProjectionResult.combine(subResults);
  }

  @Nonnull
  @Override
  public String toString() {
    return "GroupingSelection{" +
        "components=[" + components.stream()
        .map(ProjectionClause::toString)
        .collect(joining(", ")) +
        "]}";
  }

  /**
   * Returns a string expression representation of this grouping selection.
   *
   * @return the string expression "group"
   */
  @Nonnull
  public String toExpression() {
    return "group";
  }

  @Override
  @Nonnull
  public String toTreeString(final int level) {
    final String indent = "  ".repeat(level);
    return indent + toExpression() + "\n" +
        components.stream()
            .map(c -> c.toTreeString(level + 1))
            .collect(joining("\n"));
  }
}

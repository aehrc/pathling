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
import org.jetbrains.annotations.NotNull;

/**
 * Groups multiple selections together using a union (concatenation).
 * <p>
 * This is the primary mechanism for combining multiple projection clauses into a single result
 * where all component results are concatenated sequentially.
 * </p>
 *
 * @param components The list of projection clauses to be combined via concatenation
 * @author John Grimes
 * @author Piotr Szul
 */
public record UnionSelection(@Nonnull List<ProjectionClause> components) implements
    ProjectionClause {

  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Evaluate each component
    final List<ProjectionResult> results = components.stream()
        .map(c -> c.evaluate(context))
        .toList();

    // Use the explicit concatenate method
    return ProjectionResult.concatenate(results);
  }

  /**
   * Returns the FHIRPath expression representation of this union selection.
   *
   * @return the expression string "union"
   */
  @Nonnull
  public String toExpression() {
    return "union";
  }

  @Override
  @Nonnull
  public @NotNull String toTreeString(final int level) {
    final String indent = "  ".repeat(level);
    return indent + toExpression() + "\n" +
        components.stream()
            .map(c -> c.toTreeString(level + 1))
            .collect(joining("\n"));
  }
}

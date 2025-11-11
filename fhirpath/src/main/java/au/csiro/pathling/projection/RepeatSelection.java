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

import static org.apache.spark.sql.functions.concat;

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;

/**
 * Represents a selection that performs recursive traversal of nested data structures using the
 * repeat directive. This enables automatic flattening of hierarchical data to any depth.
 *
 * @param paths the list of FHIRPath expressions that define paths to recursively traverse
 * @param components the list of components to select from the recursively collected nodes
 * @author Piotr Szul
 */
public record RepeatSelection(
    @Nonnull List<FhirPath> paths,
    @Nonnull List<ProjectionClause> components
) implements ProjectionClause {

  private static final int DEF_MAX_DEPTH = 10;

  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {

    final ProjectionEvalHelper evalHelper = new ProjectionEvalHelper(components);

    // create the list of the  non-empty starting context the current context and provided paths
    final List<ProjectionContext> startingNodes = paths.stream()
        .map(context::evalExpression)
        .filter(Collection::isNotEmpty)
        .map(context::withInputContext)
        .toList();

    // then we map them to transformTree expressions and contact the results
    final Column[] nodeResults = startingNodes.stream()
        .map(ctx -> ValueFunctions.transformTree(
                ctx.inputContext().getColumnValue(),
                c -> evalHelper.evalForEach(ctx.withInputColumn(c)),
                paths.stream().map(ctx::asColumnOperator).toList(),
                DEF_MAX_DEPTH
            )
        ).toArray(Column[]::new);

    final Column result = nodeResults.length > 0
                          ? concat(nodeResults)
                          : evalHelper.evalForEach(context.withEmptyInput());

    // compute the output schema based on the first non-empty starting context or an empty context
    final ProjectionContext schemaContext = startingNodes.stream()
        .findFirst()
        .orElse(context.withEmptyInput());

    final List<ProjectedColumn> columnDescriptors = evalHelper.getResultSchema(schemaContext);
    // Return a new projection result from the column result and the column descriptors
    return ProjectionResult.of(columnDescriptors, result);
  }

  @Nonnull
  @Override
  public String toString() {
    return "RepeatSelection{" +
        "paths=[" + paths.stream()
        .map(FhirPath::toExpression)
        .collect(Collectors.joining(", ")) +
        "], components=[" + components.stream()
        .map(ProjectionClause::toString)
        .collect(Collectors.joining(", ")) +
        "]}";
  }

  /**
   * Returns the FHIRPath expression representation of this repeat selection.
   *
   * @return the expression string containing repeat with paths
   */
  @Nonnull
  public String toExpression() {
    return "repeat: [" + paths.stream()
        .map(FhirPath::toExpression)
        .collect(Collectors.joining(", ")) + "]";
  }

  @Override
  @Nonnull
  public String toTreeString(final int level) {
    final String indent = "  ".repeat(level);
    return indent + toExpression() + "\n" +
        components.stream()
            .map(c -> c.toTreeString(level + 1))
            .collect(Collectors.joining("\n"));
  }
}

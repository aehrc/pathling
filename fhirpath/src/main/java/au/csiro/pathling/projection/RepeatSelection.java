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

import static org.apache.spark.sql.functions.concat;

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;

/**
 * Represents a selection that performs recursive traversal of nested data structures using the
 * repeat directive.
 *
 * <p>This enables automatic flattening of hierarchical data to any depth by recursively following
 * the specified paths and applying a projection clause at each level. When multiple projections are
 * needed, wrap them in a {@link GroupingSelection} first.
 *
 * @param paths the list of FHIRPath expressions that define paths to recursively traverse
 * @param component the projection clause to apply at each level (use GroupingSelection for
 *     multiple)
 * @param maxDepth the maximum depth for self-referencing structure traversals
 * @author Piotr Szul
 */
public record RepeatSelection(
    @Nonnull List<FhirPath> paths, @Nonnull ProjectionClause component, int maxDepth)
    implements UnarySelection {

  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {

    // Create the list of non-empty starting contexts from current context and provided paths
    final List<ProjectionContext> startingNodes =
        paths.stream()
            .map(context::evalExpression)
            .filter(Collection::isNotEmpty)
            .map(context::withInputContext)
            .toList();

    // Map starting nodes to transformTree expressions and concatenate the results
    final Column[] nodeResults =
        startingNodes.stream()
            .map(
                ctx ->
                    ValueFunctions.transformTree(
                        ctx.inputContext().getColumnValue(),
                        c ->
                            ValueFunctions.emptyArrayIfMissingField(
                                component.evaluateElementWise(ctx.withInputColumn(c))),
                        paths.stream().map(ctx::asColumnOperator).toList(),
                        maxDepth))
            .toArray(Column[]::new);

    final Column result =
        nodeResults.length > 0
            ? concat(nodeResults)
            : DefaultRepresentation.empty()
                .plural()
                .transform(component.asColumnOperator(context.withEmptyInput()))
                .flatten()
                .getValue();

    // Compute the output schema based on first non-empty context or empty context
    final ProjectionContext schemaContext =
        startingNodes.stream().findFirst().orElse(context.withEmptyInput());

    return component.evaluate(schemaContext).withResultColumn(result);
  }

  /**
   * Returns the FHIRPath expression representation of this repeat selection.
   *
   * @return the expression string containing repeat with paths
   */
  @Nonnull
  @Override
  public String toExpression() {
    return "repeat: ["
        + paths.stream().map(FhirPath::toExpression).collect(Collectors.joining(", "))
        + "]";
  }
}

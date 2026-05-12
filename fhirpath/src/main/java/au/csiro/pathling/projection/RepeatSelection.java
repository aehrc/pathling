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

import au.csiro.pathling.encoders.RowIndexCounter;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a selection that performs recursive traversal of nested data structures using the
 * repeat directive.
 *
 * <p>This enables automatic flattening of hierarchical data to any depth by recursively following
 * the specified paths and applying a projection clause at each level. When multiple projections are
 * needed, wrap them in a {@link GroupingSelection} first.
 *
 * <p>When same-type depth is exhausted, Extension traversals silently stop and return results
 * collected up to that point. All other types raise a runtime error indicating that the recursive
 * traversal exceeded the maximum depth.
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

    // Create a shared counter for the %rowIndex environment variable. The counter is split into
    // read and increment operations: all %rowIndex references within a single element read the
    // same value (via rowCounterGet), and the counter advances exactly once per element (via
    // rowCounterIncrement wrapping the extractor result).
    final RowIndexCounter rowIndexCounter = new RowIndexCounter();
    final Column rowIndexCol = ValueFunctions.rowCounterGet(rowIndexCounter);

    // Evaluate each path to get collections, retaining them for type inspection.
    final List<Collection> pathCollections = paths.stream().map(context::evalExpression).toList();

    // Determine whether depth exhaustion should error. Error unless all paths produce Extension
    // types. If any path is non-Extension (or has no FHIR type), depth exhaustion raises an error.
    final boolean errorOnDepthExhaustion =
        pathCollections.stream()
            .filter(Collection::isNotEmpty)
            .anyMatch(
                c -> c.getFhirType().map(t -> !FHIRDefinedType.EXTENSION.equals(t)).orElse(true));

    // Create the list of non-empty starting contexts from the evaluated path collections. The row
    // index counter is injected so that %rowIndex resolves to the global element position.
    final List<ProjectionContext> startingNodes =
        pathCollections.stream()
            .filter(Collection::isNotEmpty)
            .map(context::withInputContext)
            .map(ctx -> ctx.withRowIndex(rowIndexCol))
            .toList();

    // Map starting nodes to transformTree expressions and concatenate the results.
    final Column[] nodeResults =
        startingNodes.stream()
            .map(
                ctx ->
                    ValueFunctions.transformTree(
                        ctx.inputContext().getColumnValue(),
                        c ->
                            ValueFunctions.emptyArrayIfMissingField(
                                evaluateElementWiseWithIncrement(
                                    ctx.withInputColumn(c), rowIndexCounter)),
                        paths.stream().map(ctx::asColumnOperator).toList(),
                        maxDepth,
                        errorOnDepthExhaustion))
            .toArray(Column[]::new);

    // Wrap the concatenated result with a counter reset so that the %rowIndex sequence restarts at
    // zero for each resource row.
    final Column result =
        nodeResults.length > 0
            ? ValueFunctions.resetCounter(concat(nodeResults), rowIndexCounter)
            : DefaultRepresentation.empty()
                .plural()
                .transform(component.asColumnOperator(context.withEmptyInput()))
                .flatten()
                .getValue();

    // Compute the output schema based on first non-empty context or empty context.
    final ProjectionContext schemaContext =
        startingNodes.stream().findFirst().orElse(context.withEmptyInput());

    return component.evaluate(schemaContext).withResultColumn(result);
  }

  /**
   * Evaluates the component clause element-wise, wrapping each per-element result with a counter
   * increment. This ensures the shared row index counter advances exactly once per array element,
   * after all {@code %rowIndex} references within that element have been read.
   *
   * @param context the projection context for evaluation
   * @param counter the shared counter to increment after each element
   * @return the resulting column after element-wise evaluation with per-element increment
   */
  @Nonnull
  private Column evaluateElementWiseWithIncrement(
      @Nonnull final ProjectionContext context, @Nonnull final RowIndexCounter counter) {
    final UnaryOperator<Column> elementOperator = component.asColumnOperator(context);
    return new DefaultRepresentation(context.inputContext().getColumnValue())
        .transform(c -> ValueFunctions.rowCounterIncrement(elementOperator.apply(c), counter))
        .flatten()
        .getValue();
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

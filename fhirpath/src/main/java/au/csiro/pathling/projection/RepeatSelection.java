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

import static au.csiro.pathling.encoders.ColumnFunctions.structProduct;
import static org.apache.spark.sql.functions.concat;

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  
  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Start with the input context as the initial level
    final Collection inputContext = context.inputContext();

    // Perform recursive traversal: at each level, apply all paths to all nodes from the previous
    // level. This implements the algorithm specified in the SQL on FHIR spec:
    // 1. Initialize result list with root nodes
    // 2. For each level: evaluate all paths on all nodes from previous level
    // 3. Union results from all levels

    // Create the list of all levels' collections
    final List<Collection> allLevels = Stream.iterate(
            // Start with a list containing just the input context
            List.of(inputContext),
            // For each level: apply all paths to all nodes from the previous level
            levelCollections -> levelCollections.stream()
                .flatMap(col -> paths.stream()
                    .map(path -> context.withInputContext(col).evalExpression(path)))
                .toList()
        )
        .limit(context.maxNestingLevel())  // Limit depth to prevent infinite recursion
        .flatMap(List::stream)  // Flatten the list of collections
        .filter(c -> c != inputContext)  // Exclude the root context from results
        .toList();

    // Evaluate components on each node from all levels
    final Column[] nodeResults = allLevels.stream()
        .map(node -> evaluateComponentsOnNode(node, context))
        .map(ValueFunctions::nullIfUnresolved)
        .map(c -> new DefaultRepresentation(c).plural().getValue())
        .toArray(Column[]::new);

    final Column result = concat(nodeResults);

    // Create a stub context to determine the types of the results
    final ProjectionContext stubContext = context.withInputContext(
        allLevels.getFirst().map(c -> DefaultRepresentation.empty()));
    final List<ProjectionResult> stubResults = components.stream()
        .map(s -> s.evaluate(stubContext))
        .toList();
    final List<ProjectedColumn> columnDescriptors = stubResults.stream()
        .flatMap(sr -> sr.getResults().stream())
        .toList();

    // Return a new projection result from the column result and the column descriptors
    return ProjectionResult.of(columnDescriptors, result);
  }

  /**
   * Evaluates the components on a single node from the recursively collected results.
   *
   * @param inputContext the collection representing a node from the recursive traversal
   * @param context the projection context
   * @return a column representing the evaluated components as a flattened array of structs
   */
  @Nonnull
  private Column evaluateComponentsOnNode(@Nonnull final Collection inputContext,
      @Nonnull final ProjectionContext context) {
    // Use the Collection's transform and flatten methods to unnest the components.
    // This approach is similar to how UnnestingSelection works.
    return inputContext.getColumn().transform(
        c -> unnestComponents(c, inputContext, context)
    ).flatten().getValue();
  }

  /**
   * Evaluates the components on a single element within a collection node.
   *
   * @param unnestingColumn the column representing a single element
   * @param unnestingCollection the collection containing the element
   * @param context the projection context
   * @return a struct combining the evaluated component columns
   */
  @Nonnull
  private Column unnestComponents(@Nonnull final Column unnestingColumn,
      @Nonnull final Collection unnestingCollection, @Nonnull final ProjectionContext context) {
    // Create a new projection context based upon the unnesting collection.
    final ProjectionContext projectionContext = context.withInputContext(
        unnestingCollection.map(c -> new DefaultRepresentation(unnestingColumn)));

    // Evaluate each of the components of the unnesting selection, and get the result columns.
    final Column[] subSelectionColumns = components.stream()
        .map(s -> s.evaluate(projectionContext).getResultColumn())
        .toArray(Column[]::new);

    // Combine the result columns into a struct.
    return structProduct(subSelectionColumns);
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

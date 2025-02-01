/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.view;

import static au.csiro.pathling.encoders.ColumnFunctions.structProduct;
import static au.csiro.pathling.encoders.ColumnFunctions.structProduct_outer;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.transform;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.column.EmptyRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.spark.sql.Column;

/**
 * Represents a selection that unnests a nested data structure, with either inner or outer join
 * semantics.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Value
public class UnnestingSelection implements ProjectionClause {

  @Nonnull
  FhirPath path;

  @Nonnull
  List<ProjectionClause> components;

  boolean joinOuter;

  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Evaluate the path to get the collection that will serve as the basis for unnesting.
    final Collection unnestingCollection = context.evalExpression(path);

    // Get the column that represents the unnesting collection.
    final Column unnestingColumn = unnestingCollection.getColumn().toArray().getValue();

    // Unnest the components of the unnesting selection.
    Column columnResult = flatten(
        transform(unnestingColumn, c -> unnestComponents(c, unnestingCollection, context)));

    if (joinOuter) {
      // If we are doing an outer join, we need to use structProduct_outer to ensure that a row is
      // always returned, even if the unnesting collection is empty.
      columnResult = structProduct_outer(columnResult);
    }

    // This is a way to evaluate the expression for the purpose of getting the types of the result.
    final ProjectionContext stubContext = context.withInputContext(
        unnestingCollection.map(__ -> EmptyRepresentation.getInstance()));
    final List<ProjectionResult> stubResults = components.stream()
        .map(s -> s.evaluate(stubContext))
        .toList();
    final List<ProjectedColumn> columnDescriptors = stubResults.stream()
        .flatMap(sr -> sr.getResults().stream())
        .toList();

    // Return a new projection result from the column result and the column descriptors.
    return ProjectionResult.of(columnDescriptors, columnResult);
  }

  @Nonnull
  private Column unnestComponents(@Nonnull final Column unnestingColumn,
      @Nonnull final Collection unnestingCollection, @Nonnull final ProjectionContext context) {
    // Create a new projection context based upon the unnesting collection.
    final ProjectionContext projectionContext = context.withInputContext(
        unnestingCollection.map(__ -> new DefaultRepresentation(unnestingColumn)));

    // Evaluate each of the components of the unnesting selection, and get the result
    // columns.
    final Column[] subSelectionColumns = components.stream()
        .map(s -> s.evaluate(projectionContext).getResultColumn())
        .toArray(Column[]::new);

    // Combine the result columns into a struct.
    return structProduct(subSelectionColumns);
  }

  @Override
  public String toString() {
    return "UnnestingSelection{" +
        "path=" + path +
        ", components=[" + components.stream()
        .map(ProjectionClause::toString)
        .collect(Collectors.joining(", ")) +
        "], joinOuter=" + joinOuter +
        '}';
  }

  @Nonnull
  public String toExpression() {
    return (joinOuter
            ? "forEachOrNull"
            : "forEach")
        + ": " + path.toExpression();
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

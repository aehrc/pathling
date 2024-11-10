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

package au.csiro.pathling.query.view.runner;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

/**
 * Creates a projection from the requested columns.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Value
public class ColumnSelection implements ProjectionClause {

  @NotNull
  List<RequestedColumn> columns;

  @Override
  @NotNull
  public ProjectionResult evaluate(@NotNull final ProjectionContext context) {
    // Get an iterator of collections and an iterator for requested columns.
    final Iterator<Collection> collectionsIterator = getCollectionIterator(context);
    final Iterator<RequestedColumn> requestedColumnsIterator = columns.iterator();

    // Create a list of ProjectedColumns, which pair a collection with a requested column.
    final List<ProjectedColumn> projectedColumns = new ArrayList<>();
    while (collectionsIterator.hasNext() && requestedColumnsIterator.hasNext()) {
      final Collection collection = collectionsIterator.next();
      final RequestedColumn requestedColumn = requestedColumnsIterator.next();
      projectedColumns.add(new ProjectedColumn(collection, requestedColumn));
    }

    // Collect the columns into an array, aliasing them with the requested names.
    final Column[] collectedColumns = projectedColumns.stream()
        .map(projectedColumn -> {
          final Column collectionColumn = projectedColumn.getCollection().getColumn()
              .getValue();
          final String requestedName = projectedColumn.getRequestedColumn().getName();
          return collectionColumn.alias(requestedName);
        })
        .toArray(Column[]::new);

    // Create a new column that is an array of structs, where each struct has a field for each
    // requested column.
    final Column resultColumn = array(struct(collectedColumns));

    // Create a new ProjectionResult with the projected columns and the result column.
    return ProjectionResult.of(projectedColumns, resultColumn);
  }

  /**
   * Evaluate each requested column to get the collection it represents.
   *
   * @param context The projection context
   * @return An iterator of collections
   */
  private @NotNull Iterator<Collection> getCollectionIterator(
      final @NotNull ProjectionContext context) {
    final Stream<Collection> collections = columns.stream()
        .map(col -> {
          final Collection collection = context.evalExpression(col.getPath());

          // If a type was asserted for the column, check that the collection is of that type.
          col.getType().ifPresent(type -> {
            if (collection.getFhirType().isPresent() && !collection.getFhirType().get()
                .equals(type)) {
              throw new IllegalArgumentException(
                  "Collection " + collection + " has type " + collection.getFhirType().get()
                      + ", expected " + type);
            }
          });

          return col.isCollection()
                 ? collection
                 : collection.asSingular();
        });

    // Zip stream of requested columns with the stream of collections, creating a ProjectedColumn 
    // for each pair.
    return collections.iterator();
  }

  @Override
  public String toString() {
    return "ColumnSelection{" +
        "columns=[" + columns.stream()
        .map(RequestedColumn::toString)
        .collect(Collectors.joining(", ")) +
        "]}";
  }

}

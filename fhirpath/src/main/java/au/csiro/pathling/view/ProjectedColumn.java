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

import au.csiro.pathling.fhirpath.External;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;

/**
 * The result of evaluating a {@link RequestedColumn} as part of a {@link ProjectionClause}.
 */
@Value
public class ProjectedColumn {

  /**
   * The result of evaluating the column.
   */
  @Nonnull
  Collection collection;

  /**
   * The column that was requested to be included in the projection.
   */
  @Nonnull
  RequestedColumn requestedColumn;

  /**
   * Gets the column value from the collection and aliases it with the requested name. If a SQL type
   * is specified in the requested column, the column value will be cast to that type.
   *
   * @return The column value with the appropriate alias
   */
  @Nonnull
  public Column getValue() {
    // If a type was asserted for the column, check that the collection is of that type.
    requestedColumn.getType().ifPresent(requestedType ->
        collection.getFhirType().ifPresent(actualType -> {
          if (!requestedType.equals(actualType)) {
            throw new IllegalArgumentException(
                "Collection " + collection + " has type " + actualType
                    + ", expected " + requestedType);
          }
        })
    );
    final Column rawResult = External.getExternalValue(requestedColumn.isCollection()
                                                       ? collection.asPlural()
                                                       : collection.asSingular());
    return requestedColumn.getSqlType()
        .map(rawResult::cast)
        .orElse(rawResult)
        .alias(requestedColumn.getName());

  }
}

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

import static au.csiro.pathling.encoders.ValueFunctions.ifArray;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.when;

import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Groups multiple selections together using a union.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Value
public class UnionSelection implements ProjectionClause {

  @Nonnull
  List<ProjectionClause> components;

  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Evaluate each component of the union.
    final List<ProjectionResult> results = components.stream()
        .map(c -> c.evaluate(context))
        .collect(toUnmodifiableList());

    // Process each result to ensure that they are all arrays.
    final Column[] converted = results.stream()
        .map(ProjectionResult::getResultColumn)
        // When the result is a singular null, convert it to an empty array.
        .map(col -> when(isnull(col), array())
            .otherwise(ifArray(col,
                // If the column is an array, return it as is.
                c -> c,
                // If the column is a singular value, convert it to an array.
                functions::array
            )))
        .toArray(Column[]::new);

    // Concatenate the converted columns.
    final Column combinedResult = concat(converted);

    return ProjectionResult.of(results.get(0).getResults(), combinedResult);
  }

}

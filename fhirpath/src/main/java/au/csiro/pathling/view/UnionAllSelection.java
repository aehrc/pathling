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

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ValueFunctions;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

@Value
public class UnionAllSelection implements SelectionX {

  @Nonnull
  List<SelectionX> components;

  @Nonnull
  @Override
  public SelectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Evaluate each component of the union.
    final List<SelectionResult> results = components.stream().map(c -> c.evaluate(context))
        .collect(toUnmodifiableList());

    // Process each result to ensure that they are all arrays.
    final Column[] converted = results.stream()
        .map(SelectionResult::getValue)
        // When the result is a singular null, convert it to an empty array.
        .map(col -> when(isnull(col), array())
            .otherwise(ValueFunctions.ifArray(col,
                // If the column is an array, return it as is.
                c -> c,
                // If the column is a singular value, convert it to an array.
                functions::array
            )))
        .toArray(Column[]::new);

    // Concatenate the converted columns.
    final Column combinedResult = concat(converted);

    return SelectionResult.of(results.get(0).getCollections(), combinedResult);
  }
}
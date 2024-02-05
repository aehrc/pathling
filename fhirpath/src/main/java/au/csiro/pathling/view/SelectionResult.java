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


import au.csiro.pathling.encoders.ColumnFunctions;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;

@Value(staticConstructor = "of")
public class SelectionResult {

  @Nonnull
  List<CollectionResult> collections;

  @Nonnull
  Column value;

  @Nonnull
  public static SelectionResult combine(@Nonnull final List<SelectionResult> results) {
    return combine(results, false);
  }

  @Nonnull
  public static SelectionResult combine(@Nonnull final List<SelectionResult> results,
      final boolean outer) {
    if (results.size() == 1 && !outer) {
      return results.get(0);
    } else {
      return of(
          results.stream().flatMap(r -> r.getCollections().stream())
              .collect(Collectors.toUnmodifiableList()),
          structProduct(outer,
              results.stream().map(SelectionResult::getValue).toArray(Column[]::new))
      );
    }
  }

  @Nonnull
  private static Column structProduct(final boolean outer, @Nonnull final Column... columns) {
    return outer
           ? ColumnFunctions.structProduct_outer(columns)
           : ColumnFunctions.structProduct(columns);
  }
}

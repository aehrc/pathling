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

import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

@Value
public class UnionAllSelection implements SelectionX {

  @Nonnull
  List<SelectionX> components;

  @Nonnull
  @Override
  public SelectionResult evaluate(@Nonnull final ProjectionContext context) {

    final List<SelectionResult> subResults = components.stream().map(c -> c.evaluate(context))
        .collect(Collectors.toUnmodifiableList());
    // combine the results with concat()
    final Column combinedResult = functions.concat(
        subResults.stream().map(SelectionResult::getValue).toArray(Column[]::new));

    return SelectionResult.of(
        subResults.get(0).getCollections(),
        combinedResult
    );
  }
}

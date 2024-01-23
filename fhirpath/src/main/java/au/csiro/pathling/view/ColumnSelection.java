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

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

@Value
public class ColumnSelection implements SelectionX {

  @Nonnull
  List<PrimitiveSelection> columns;
  
  @Nonnull
  @Override
  public List<CollectionResult> evaluateFlat(@Nonnull final ProjectionContext context) {
    return columns.stream()
        .map(c -> c.evaluateCollection(context)).collect(Collectors.toUnmodifiableList());
  }

  @Override
  @Nonnull
  public SelectionResult evaluate(@Nonnull final ProjectionContext context) {

    // run all column selections
    final List<CollectionResult> resultCollections = evaluateFlat(context);

    // commpose the result to an array struct
    final Column resultRow = functions.array(
        functions.struct(resultCollections.stream().map(CollectionResult::getTaggedColumn)
            .toArray(Column[]::new))
    );
    // I now also need to remap the results to be bound to the correct fields
    return SelectionResult.of(
        resultCollections.stream().map(CollectionResult::toTagReference).collect(
            Collectors.toUnmodifiableList()), resultRow);
  }

}

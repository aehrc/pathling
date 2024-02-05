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
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.column.StdColumnCtx;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

@Value
public class ForEachSelectionX implements SelectionX {

  @Nonnull
  FhirPath path;

  @Nonnull
  List<SelectionX> components;

  boolean withNulls;

  @Nonnull
  @Override
  public SelectionResult evaluate(@Nonnull final ProjectionContext context) {

    final Collection nestedInputContext = context.evalExpression(path).getPureValue();

    // here we need to deal better values that are not nested
    Column columnResult = functions.flatten(
        functions.transform(
            nestedInputContext.getColumnCtx().toArray().getValue(),
            c -> {
              // create the transformation element subcontext
              final ProjectionContext elementCtx = context.withInputContext(
                  nestedInputContext.map(__ -> StdColumnCtx.of(c)));
              return ColumnFunctions.structProduct(
                  components.stream()
                      .map(s -> s.evaluate(elementCtx).getValue())
                      .toArray(Column[]::new));
            }
        )
    );
    if (withNulls) {
      columnResult = ColumnFunctions.structProduct_outer(columnResult);
    }

    // This is a way to evaluate the expression for the purpose of getting the types of the result.
    final ProjectionContext stubContext = context.withInputContext(
        nestedInputContext.map(__ -> ColumnCtx.nullCtx()));
    final List<SelectionResult> stubResults = components.stream().map(s -> s.evaluate(stubContext))
        .collect(Collectors.toUnmodifiableList());

    return SelectionResult.of(
        stubResults.stream().flatMap(sr -> sr.getCollections().stream()).collect(
            Collectors.toUnmodifiableList()), columnResult);
  }

}

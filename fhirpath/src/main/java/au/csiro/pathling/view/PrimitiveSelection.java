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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;

@Value
public class PrimitiveSelection implements Selection {

  FhirPath<Collection> path;
  Function<Collection, Column> valueExtractor;

  public PrimitiveSelection(@Nonnull final FhirPath<Collection> path) {
    this.path = path;
    this.valueExtractor = ColumnExtractor.UNCONSTRAINED;
  }

  @Override
  public DatasetView evaluate(@Nonnull final ProjectionContext context) {
    return DatasetView.of(context.evalExpression(path, false));
  }

  @Override
  public void printTree(final int i) {
    System.out.println("  ".repeat(i) + "select: " + path + " with " + valueExtractor);
  }

  @Nonnull
  @Override
  public Selection map(@Nonnull final Function<Selection, Selection> mapFunction) {
    return mapFunction.apply(this);
  }
}

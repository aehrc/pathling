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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.spark.sql.Column;

@Value
@AllArgsConstructor
public class PrimitiveSelection implements Selection {


  @Nonnull
  FhirPath<Collection> path;

  @Nonnull
  Function<Collection, Column> valueExtractor;

  @Nonnull
  Optional<String> alias;

  boolean asCollection;

  public PrimitiveSelection(@Nonnull final FhirPath<Collection> path) {
    this(path, Optional.empty());
  }

  public PrimitiveSelection(@Nonnull final FhirPath<Collection> path,
      @Nonnull final Optional<String> alias) {
    this(path, alias, false);

  }

  public PrimitiveSelection(@Nonnull final FhirPath<Collection> path,
      @Nonnull final Optional<String> alias, final boolean asCollection) {
    this(path, ColumnExtractor.UNCONSTRAINED, alias, asCollection);
  }

  @Override
  public DatasetResult<Column> evaluate(@Nonnull final ProjectionContext context) {
    final Column resultColumn = context.evalExpression(path, !asCollection);
    return DatasetResult.of(alias.map(resultColumn::alias).orElse(resultColumn));
  }

  @Override
  public Stream<String> toTreeString() {
    return Stream.of("select: " + path + " with " + valueExtractor + " as " + alias);
  }

  @Nonnull
  @Override
  public Selection map(@Nonnull final Function<Selection, Selection> mapFunction) {
    return mapFunction.apply(this);
  }
}

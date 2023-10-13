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
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Column;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
@NonFinal
@AllArgsConstructor
public abstract class AbstractCompositeSelection implements Selection {

  protected FhirPath<Collection> path;
  List<Selection> components;

  @Override
  public DatasetResult<Column> evaluate(@Nonnull final ProjectionContext context) {
    final Pair<ProjectionContext, DatasetResult<Column>> pathContext = subContext(context, path);
    return components.stream().map(s -> s.evaluate(pathContext.getLeft()))
        .reduce(pathContext.getRight(), DatasetResult::andThen);
  }

  @Override
  public Stream<String> toTreeString() {
    return Stream.concat(
        Stream.of(getName() + ":  " + path),
        components.stream().flatMap(c -> c.toTreeString().map(s -> "  " + s))
    );
  }

  @Nonnull
  @Override
  public Selection map(@Nonnull final Function<Selection, Selection> mapFunction) {
    final List<Selection> newComponents = components.stream().map(c -> c.map(mapFunction))
        .collect(Collectors.toUnmodifiableList());
    return components.equals(newComponents)
           ? mapFunction.apply(this)
           : mapFunction.apply(copy(newComponents));
  }

  @Nonnull
  protected abstract String getName();

  @Nonnull
  protected abstract AbstractCompositeSelection copy(
      @Nonnull final List<Selection> newComponents);

  @Nonnull
  protected Pair<ProjectionContext, DatasetResult<Column>> subContext(
      @Nonnull final ProjectionContext context,
      @Nonnull final FhirPath<Collection> parent) {
    return context.subContext(parent);
  }


}

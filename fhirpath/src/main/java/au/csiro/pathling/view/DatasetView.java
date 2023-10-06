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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Encapsulates the result of a view query.
 */
public interface DatasetView {

  Empty EMPTY = new Empty();

  @Nonnull
  Stream<Column> asStream();

  @Nonnull
  Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform();


  @Nonnull
  default DatasetView andThen(@Nonnull final DatasetView next) {
    return new ProjectionView(
        Stream.concat(this.asStream(), next.asStream())
            .collect(Collectors.toUnmodifiableList()),
        // TODOmaybe just use identity() here
        getTransform().map(t -> next.getTransform().map(t::andThen).orElse(t))
            .or(next::getTransform)
    );
  }

  @Nonnull
  default Dataset<Row> apply(@Nonnull final Dataset<Row> dataset) {
    return getTransform().map(t -> t.apply(dataset)).orElse(dataset)
        .select(asStream().toArray(Column[]::new));
  }

  @Nonnull
  static DatasetView empty() {
    return EMPTY;
  }

  @Nonnull
  static DatasetView of(@Nonnull final Column column) {
    return new OneColumn(column, Optional.empty());
  }

  default DatasetView asTransform() {
    return new Transform(getTransform());
  }

  @Value
  class Empty implements DatasetView {

    @Nonnull
    @Override
    public Stream<Column> asStream() {
      return Stream.empty();
    }

    @Nonnull
    @Override
    public Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform() {
      return Optional.empty();
    }
  }

  @Value
  class OneColumn implements DatasetView {

    Column column;
    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @Nonnull
    @Override
    public Stream<Column> asStream() {
      return Stream.of(column);
    }
  }

  @Value
  class Transform implements DatasetView {

    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @Nonnull
    @Override
    public Stream<Column> asStream() {
      return Stream.empty();
    }

  }


  @Value
  class ProjectionView implements DatasetView {

    List<Column> columns;
    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @Nonnull
    @Override
    public Stream<Column> asStream() {
      return columns.stream();
    }
  }

}

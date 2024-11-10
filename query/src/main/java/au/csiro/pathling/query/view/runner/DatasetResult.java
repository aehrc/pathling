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

package au.csiro.pathling.query.view.runner;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates the result of a view query.
 */
public interface DatasetResult<T> {

  Empty<?> EMPTY = new Empty<>();

  @NotNull
  Stream<T> asStream();

  @NotNull
  Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform();

  @NotNull
  default DatasetResult<T> andThen(@NotNull final DatasetResult<T> next) {
    return new Composite<>(
        Stream.concat(this.asStream(), next.asStream())
            .collect(Collectors.toUnmodifiableList()),
        // TODOmaybe just use identity() here
        getTransform().map(t -> next.getTransform().map(t::andThen).orElse(t))
            .or(next::getTransform)
    );
  }

  @NotNull
  default Dataset<Row> applyTransform(@NotNull final Dataset<Row> dataset) {
    return getTransform().map(t -> t.apply(dataset)).orElse(dataset);
  }

  <K> DatasetResult<K> map(@NotNull final Function<T, K> mapper);

  @NotNull
  static <T> DatasetResult<T> empty() {
    //noinspection unchecked
    return (DatasetResult<T>) EMPTY;
  }

  @NotNull
  static <T> One<T> pureOne(@NotNull final T value) {
    return new One<>(value, Optional.empty());
  }


  @NotNull
  static <T> One<T> one(@NotNull final T value,
      @NotNull final Function<Dataset<Row>, Dataset<Row>> transform) {
    return new One<>(value, Optional.of(transform));
  }

  @NotNull
  static <T> DatasetResult<T> fromTransform(
      @NotNull final Function<Dataset<Row>, Dataset<Row>> transform) {
    return new Transform<>(transform);
  }

  default DatasetResult<T> asTransform() {
    //noinspection unchecked
    return (DatasetResult<T>) getTransform().map(DatasetResult::fromTransform).orElse(empty());
  }


  default <K> DatasetResult<K> asAnyTransform() {
    //noinspection unchecked
    return (DatasetResult<K>) getTransform().map(DatasetResult::fromTransform).orElse(empty());
  }

  // Column Based operations
  @NotNull
  default Dataset<Row> select(@NotNull final Dataset<Row> dataset,
      @NotNull final Function<T, Column> asColumn) {
    return getTransform().map(t -> t.apply(dataset)).orElse(dataset)
        .select(asStream()
            .map(asColumn)
            .toArray(Column[]::new));
  }

  default DatasetResult<Column> toFilter(@NotNull final Function<T, Column> asColumn) {
    final List<Column> filterColumns = asStream()
        .map(asColumn)
        .collect(Collectors.toUnmodifiableList());
    return filterColumns.isEmpty()
           ? DatasetResult.empty()
           : this.<Column>asAnyTransform().andThen(fromTransform(ds -> ds.filter(
               filterColumns.stream()
                   .reduce(Column::and).orElseThrow()
           )));
  }


  @Value
  class Empty<T> implements DatasetResult<T> {

    @NotNull
    @Override
    public Stream<T> asStream() {
      return Stream.empty();
    }

    @NotNull
    @Override
    public Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform() {
      return Optional.empty();
    }

    @Override
    public <K> DatasetResult<K> map(@NotNull final Function<T, K> mapper) {
      return empty();
    }
  }

  @Value
  class One<T> implements DatasetResult<T> {

    T value;
    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @NotNull
    @Override
    public Stream<T> asStream() {
      return Stream.of(value);
    }

    @NotNull
    public One<T> withTransformOf(@NotNull final DatasetResult<T> other) {
      return new One<>(value, other.andThen(this).asTransform().getTransform());
    }

    @NotNull
    public T getPureValue() {
      if (transform.isPresent()) {
        throw new IllegalStateException("Cannot get pure value from transformed result");
      }
      return value;
    }

    @NotNull
    public <R> One<R> flatMap(@NotNull final Function<T, One<R>> mapper) {
      return mapper.apply(value).withTransformOf(this.asAnyTransform());
    }

    @Override
    public <K> One<K> map(@NotNull final Function<T, K> mapper) {
      return new One<>(mapper.apply(value), transform);
    }

  }

  @Value
  class Transform<T> implements DatasetResult<T> {

    Function<Dataset<Row>, Dataset<Row>> transform;

    @Override
    @NotNull
    public Stream<T> asStream() {
      return Stream.empty();
    }

    @Override
    @NotNull
    public Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform() {
      return Optional.of(transform);
    }

    @Override
    public <K> DatasetResult<K> map(@NotNull final Function<T, K> mapper) {
      return asAnyTransform();
    }
  }

  @Value
  class Composite<T> implements DatasetResult<T> {

    List<T> columns;
    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @NotNull
    @Override
    public Stream<T> asStream() {
      return columns.stream();
    }

    @Override
    public <K> Composite<K> map(@NotNull final Function<T, K> mapper) {
      return new Composite<>(columns.stream().map(mapper).collect(Collectors.toUnmodifiableList()),
          transform);
    }
  }
}

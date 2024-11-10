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

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Encapsulates the result of a view query.
 */
public interface DatasetResult<T> {

  Empty<?> EMPTY = new Empty<>();

  @Nonnull
  Stream<T> asStream();

  @Nonnull
  Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform();

  @Nonnull
  default DatasetResult<T> andThen(@Nonnull final DatasetResult<T> next) {
    return new Composite<>(
        Stream.concat(this.asStream(), next.asStream())
            .collect(Collectors.toUnmodifiableList()),
        // TODOmaybe just use identity() here
        getTransform().map(t -> next.getTransform().map(t::andThen).orElse(t))
            .or(next::getTransform)
    );
  }

  @Nonnull
  default Dataset<Row> applyTransform(@Nonnull final Dataset<Row> dataset) {
    return getTransform().map(t -> t.apply(dataset)).orElse(dataset);
  }

  <K> DatasetResult<K> map(@Nonnull final Function<T, K> mapper);

  @Nonnull
  static <T> DatasetResult<T> empty() {
    //noinspection unchecked
    return (DatasetResult<T>) EMPTY;
  }

  @Nonnull
  static <T> One<T> pureOne(@Nonnull final T value) {
    return new One<>(value, Optional.empty());
  }


  @Nonnull
  static <T> One<T> one(@Nonnull final T value,
      @Nonnull final Function<Dataset<Row>, Dataset<Row>> transform) {
    return new One<>(value, Optional.of(transform));
  }

  @Nonnull
  static <T> DatasetResult<T> fromTransform(
      @Nonnull final Function<Dataset<Row>, Dataset<Row>> transform) {
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
  @Nonnull
  default Dataset<Row> select(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Function<T, Column> asColumn) {
    return getTransform().map(t -> t.apply(dataset)).orElse(dataset)
        .select(asStream()
            .map(asColumn)
            .toArray(Column[]::new));
  }

  default DatasetResult<Column> toFilter(@Nonnull final Function<T, Column> asColumn) {
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

    @Nonnull
    @Override
    public Stream<T> asStream() {
      return Stream.empty();
    }

    @Nonnull
    @Override
    public Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform() {
      return Optional.empty();
    }

    @Override
    public <K> DatasetResult<K> map(@Nonnull final Function<T, K> mapper) {
      return empty();
    }
  }

  @Value
  class One<T> implements DatasetResult<T> {

    T value;
    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @Nonnull
    @Override
    public Stream<T> asStream() {
      return Stream.of(value);
    }

    @Nonnull
    public One<T> withTransformOf(@Nonnull final DatasetResult<T> other) {
      return new One<>(value, other.andThen(this).asTransform().getTransform());
    }

    @Nonnull
    public T getPureValue() {
      if (transform.isPresent()) {
        throw new IllegalStateException("Cannot get pure value from transformed result");
      }
      return value;
    }

    @Nonnull
    public <R> One<R> flatMap(@Nonnull final Function<T, One<R>> mapper) {
      return mapper.apply(value).withTransformOf(this.asAnyTransform());
    }

    @Override
    public <K> One<K> map(@Nonnull final Function<T, K> mapper) {
      return new One<>(mapper.apply(value), transform);
    }

  }

  @Value
  class Transform<T> implements DatasetResult<T> {

    Function<Dataset<Row>, Dataset<Row>> transform;

    @Override
    @Nonnull
    public Stream<T> asStream() {
      return Stream.empty();
    }

    @Override
    @Nonnull
    public Optional<Function<Dataset<Row>, Dataset<Row>>> getTransform() {
      return Optional.of(transform);
    }

    @Override
    public <K> DatasetResult<K> map(@Nonnull final Function<T, K> mapper) {
      return asAnyTransform();
    }
  }

  @Value
  class Composite<T> implements DatasetResult<T> {

    List<T> columns;
    Optional<Function<Dataset<Row>, Dataset<Row>>> transform;

    @Nonnull
    @Override
    public Stream<T> asStream() {
      return columns.stream();
    }

    @Override
    public <K> Composite<K> map(@Nonnull final Function<T, K> mapper) {
      return new Composite<>(columns.stream().map(mapper).collect(Collectors.toUnmodifiableList()),
          transform);
    }
  }
}

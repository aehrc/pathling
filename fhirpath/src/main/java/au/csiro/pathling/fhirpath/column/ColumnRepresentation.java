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

package au.csiro.pathling.fhirpath.column;

import static au.csiro.pathling.utilities.Functions.maybeCast;
import static au.csiro.pathling.utilities.Strings.randomAlias;

import au.csiro.pathling.view.DatasetResult;
import au.csiro.pathling.view.DatasetResult.One;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.ArrayJoin;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;


public abstract class ColumnRepresentation {

  static final Column NULL_LITERAL = functions.lit(null);

  @Nonnull
  public ColumnRepresentation call(@Nonnull final Function<Column, Column> lambda) {
    return copyOf(lambda.apply(getValue()));
  }

  @Nonnull
  public static ColumnRepresentation nullCtx() {
    return NullRepresentation.INSTANCE;
  }

  public static ColumnRepresentation literal(@Nonnull final Object value) {
    return ArrayRepresentation.of(functions.lit(value));
  }

  public abstract Column getValue();

  protected abstract ColumnRepresentation copyOf(@Nonnull final Column newValue);

  @Nonnull
  public abstract ColumnRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression);


  @Nonnull
  public abstract ColumnRepresentation flatten();

  @Nonnull
  public abstract ColumnRepresentation traverse(@Nonnull final String fieldName);

  public Optional<String> asStringValue() {
    return Optional.of(getValue().expr())
        .flatMap(maybeCast(Literal.class))
        .map(Literal::toString);
  }

  @Nonnull
  public ColumnRepresentation toArray() {
    return vectorize(
        Function.identity(),
        c -> functions.when(c.isNotNull(), functions.array(c))
    );
  }

  @Nonnull
  public ColumnRepresentation combine(@Nonnull final ColumnRepresentation other) {
    return copyOf(functions.concat(toArray().getValue(), other.toArray().getValue()));
  }

  @Nonnull
  public abstract ColumnRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression);

  @Nonnull
  public ColumnRepresentation orElse(@Nonnull final Object value) {
    return copyOf(functions.coalesce(getValue(), functions.lit(value)));
  }

  @Nonnull
  public ColumnRepresentation singular() {
    return vectorize(
        c -> functions.when(functions.size(c).leq(1), c.getItem(0))
            .otherwise(functions.raise_error(
                functions.lit("Expected a single value, but found multiple values"))),
        Function.identity()
    );
  }

  @Nonnull
  public ColumnRepresentation filter(@Nonnull final Function<Column, Column> lambda) {
    return vectorize(
        c -> functions.filter(c, lambda::apply),
        c -> functions.when(c.isNotNull(), functions.when(lambda.apply(c), c))
    );
  }

  @Nonnull
  public ColumnRepresentation rlike(@Nonnull final String regex) {
    return copyOf(getValue().rlike(regex));
  }

  @Nonnull
  public ColumnRepresentation removeNulls() {
    return vectorize(
        c -> functions.filter(c, Column::isNotNull),
        Function.identity()
    );
  }


  @Nonnull
  public ColumnRepresentation transform(final Function<Column, Column> lambda) {
    return vectorize(
        c -> functions.transform(c, lambda::apply),
        c -> functions.when(c.isNotNull(), lambda.apply(c))
    );
  }

  @Nonnull
  public ColumnRepresentation aggregate(@Nonnull final Object zeroValue,
      final BiFunction<Column, Column, Column> aggregator) {

    return vectorize(
        c -> functions.when(c.isNull(), zeroValue)
            .otherwise(functions.aggregate(c, functions.lit(zeroValue), aggregator::apply)),
        c -> functions.when(c.isNull(), zeroValue).otherwise(c)
    );
    // this is OK because aggregator(zero, x) == x
  }


  @Nonnull
  public ColumnRepresentation first() {

    return vectorize(c -> c.getItem(0), Function.identity());
  }


  public ColumnRepresentation last() {
    // we need to use `element_at()` here are `getItem()` does not support column arguments
    // NOTE: `element_at()` is 1-indexed as opposed to `getItem()` which is 0-indexed
    return vectorize(
        c -> functions.when(c.isNull().or(functions.size(c).equalTo(0)), null)
            .otherwise(functions.element_at(c, functions.size(c))),
        Function.identity()
    );
  }


  @Nonnull
  public ColumnRepresentation count() {
    return vectorize(
        c -> functions.when(c.isNull(), 0).otherwise(functions.size(c)),
        c -> functions.when(c.isNull(), 0).otherwise(1)
    );
  }

  @Nonnull
  public ColumnRepresentation empty() {
    return vectorize(
        c -> functions.when(c.isNotNull(), functions.size(c).equalTo(0)).otherwise(true),
        Column::isNull);
  }

  @Nonnull
  public ColumnRepresentation join(@Nonnull final ColumnRepresentation separator) {
    return vectorize(c -> new Column(new ArrayJoin(c.expr(), separator.getValue().expr())),
        Function.identity());
  }

  @Nonnull
  public ColumnRepresentation not() {
    return transform(functions::not);
  }

  @Nonnull
  public ColumnRepresentation sum() {
    return aggregate(0, Column::plus);
  }


  @Nonnull
  public ColumnRepresentation max() {
    return vectorize(functions::array_max, Function.identity());
  }

  @Nonnull
  public ColumnRepresentation min() {
    return vectorize(functions::array_min, Function.identity());
  }

  @Nonnull
  public ColumnRepresentation allTrue() {
    return min().orElse(true);
  }

  @Nonnull
  public ColumnRepresentation allFalse() {
    return max().not().orElse(true);
  }

  @Nonnull
  public ColumnRepresentation anyTrue() {
    return max().orElse(false);
  }

  @Nonnull
  public ColumnRepresentation anyFalse() {
    return min().not().orElse(false);
  }

  /**
   * Call udf with this column as the first argument.
   */
  @Nonnull
  public ColumnRepresentation mapWithUDF(@Nonnull final String udfName,
      @Nonnull final ColumnRepresentation... args) {
    return transform(c -> functions.callUDF(udfName,
        Stream.concat(Stream.of(c), Stream.of(args).map(ColumnRepresentation::getValue))
            .toArray(Column[]::new)));

  }

  @Nonnull
  public ColumnRepresentation callUDF(@Nonnull final String udfName,
      @Nonnull final ColumnRepresentation... args) {
    return copyOf(functions.callUDF(udfName,
        Stream.concat(Stream.of(getValue()), Stream.of(args).map(ColumnRepresentation::getValue))
            .toArray(Column[]::new)));
  }

  @Nonnull
  public ColumnRepresentation cast(@Nonnull final DataType dataType) {
    return copyOf(getValue().cast(dataType));
  }


  @Nonnull
  public ColumnRepresentation asString() {
    return cast(DataTypes.StringType);
  }


  @Nonnull
  public One<ColumnRepresentation> explode() {
    //  TODO: this actually cannot should return DatasetResult as filtering is required here
    final ColumnRepresentation exploded = vectorize(functions::explode, Function.identity());
    final String materializedColumnName = randomAlias();
    return DatasetResult.one(copyOf(functions.col(materializedColumnName)),
        ds -> ds.withColumn(
                materializedColumnName, exploded.getValue())
            .filter(functions.col(materializedColumnName).isNotNull()));
  }

  @Nonnull
  public One<ColumnRepresentation> explode_outer() {
    //  TODO: this actually cannot should return DatasetResult as filtering is required here
    final ColumnRepresentation exploded = vectorize(functions::explode_outer, Function.identity());
    final String materializedColumnName = randomAlias();
    return DatasetResult.one(copyOf(functions.col(materializedColumnName)),
        ds -> ds.withColumn(
            materializedColumnName, exploded.getValue()));
  }


  @Nonnull
  public static ColumnRepresentation biOperator(@Nonnull final ColumnRepresentation left,
      @Nonnull final ColumnRepresentation right,
      @Nonnull final BiFunction<Column, Column, Column> lambda) {
    return ArrayRepresentation.of(lambda.apply(left.getValue(), right.getValue()));
  }

}

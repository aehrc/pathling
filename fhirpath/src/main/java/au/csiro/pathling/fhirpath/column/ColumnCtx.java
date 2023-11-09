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

import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.column.ColumnPaths.Call;
import au.csiro.pathling.fhirpath.column.ColumnPaths.GetField;
import au.csiro.pathling.fhirpath.column.ColumnPaths.VectorizedCall;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


public class ColumnCtx {


  @Nonnull
  final ColumnPath path;
  private static final ColumnCtx NULL_CTC = new ColumnCtx(ColumnPath.nullPath());

  protected ColumnCtx(@Nonnull final ColumnPath path) {
    this.path = path;
  }

  protected ColumnCtx() {
    this(ColumnPath.nullPath());
  }


  private ColumnCtx andThen(@Nonnull final ColumnPath nextPath) {
    return new ColumnCtx(path.andThen(nextPath));
  }

  @Nonnull
  @Deprecated
  public static ColumnCtx of(@Nonnull Column column) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  protected static ColumnCtx of(@Nonnull ColumnPath columnPath) {
    return new ColumnCtx(columnPath);
  }

  @Nonnull
  public static ColumnCtx nullCtx() {
    return NULL_CTC;
  }

  @Nonnull
  public static ColumnCtx literal(@Nonnull final Object value) {
    return of(new ColumnPaths.Literal(value));
  }

  @Nonnull
  public static ColumnCtx resource(@Nonnull final ResourceType resourceType) {
    return of(new ColumnPaths.Resource(resourceType));
  }

  @Nonnull
  public static ColumnCtx biOperator(@Nonnull final ColumnCtx left, @Nonnull final ColumnCtx right,
      @Nonnull final BiFunction<Column, Column, Column> operator) {
    return of(new ColumnPaths.Operator2(left.path, right.path, operator));
  }

  @Nonnull
  public Optional<Object> asLiteralValue() {
    return Optional.of(path)
        .filter(ColumnPaths.Literal.class::isInstance)
        .map(ColumnPaths.Literal.class::cast)
        .map(ColumnPaths.Literal::getValue);
  }

  @Nonnull
  public Optional<String> asStringValue() {
    return asLiteralValue()
        .filter(String.class::isInstance)
        .map(Object::toString);
  }

  @Nonnull
  @NotImplemented
  public ColumnCtx reverseResolve(@Nonnull final ResourceType resourceType,
      @Nonnull final ResourceType foreignResourceType) {
    return of(new ColumnPaths.ReverseJoin(resourceType, foreignResourceType));
  }

  @Nonnull
  public ColumnCtx resolve(@Nonnull final ResourceType foreignResourceType) {
    return of(new ColumnPaths.Join(foreignResourceType));
  }

  public ColumnCtx call(@Nonnull final Function<Column, Column> mapper) {
    return of(new Call(mapper));
  }

  @Nonnull
  public ColumnCtx getField(@Nonnull final String fieldName) {
    return andThen(new GetField(fieldName));
  }

  @Nonnull
  public ColumnCtx cast(@Nonnull final DataType stringType) {
    return call(c -> c.cast(stringType));
  }

  @Nonnull
  public ColumnCtx asString() {
    return cast(DataTypes.StringType);
  }


  @Nonnull
  public ColumnCtx rlike(@Nonnull final String regex) {
    return call(c -> c.rlike(regex));
  }

  public Column getValue() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  public ColumnCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return of(new VectorizedCall(arrayExpression, singularExpression));
  }

  @Nonnull
  public ColumnCtx flatten() {
    return call(ValueFunctions::unnest);
  }

  @Nonnull
  public ColumnCtx toArray() {
    return vectorize(
        Function.identity(),
        c -> functions.when(c.isNotNull(), functions.array(c))
    );
  }

  @Nonnull
  public ColumnCtx combine(@Nonnull final ColumnCtx other) {
    return biOperator(this, other, (l, r) -> functions.concat(l, r));
  }

  @SuppressWarnings("unused")
  @Nonnull
  public ColumnCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression) {
    // the default implementation just wraps the element info array if needed
    return vectorize(arrayExpression,
        c -> arrayExpression.apply(functions.when(c.isNotNull(), functions.array(c))));
  }

  @Nonnull
  public ColumnCtx orElse(@Nonnull final Object value) {
    throw new UnsupportedOperationException();
    //return copyOf(functions.coalesce(getValue(), functions.lit(value)));
  }

  @Nonnull
  public ColumnCtx singular() {
    return vectorize(
        c -> functions.when(functions.size(c).leq(1), c.getItem(0))
            .otherwise(functions.raise_error(
                functions.lit("Expected a single value, but found multiple values"))),
        Function.identity()
    );
  }

  @Nonnull
  @NotImplemented
  public ColumnCtx filter(final Function<ColumnCtx, ColumnCtx> lambda) {
    throw new UnsupportedOperationException();
    // return vectorize(
    //     c -> functions.filter(c, lambda::apply),
    //     c -> functions.when(c.isNotNull(), functions.when(lambda.apply(c), c))
    // );
  }

  @Nonnull
  public ColumnCtx removeNulls() {
    return vectorize(
        c -> functions.filter(c, Column::isNotNull),
        Function.identity()
    );
  }


  @Nonnull
  public ColumnCtx transform(final Function<Column, Column> lambda) {
    return vectorize(
        c -> functions.transform(c, lambda::apply),
        c -> functions.when(c.isNotNull(), lambda.apply(c))
    );
  }

  @Nonnull
  public ColumnCtx aggregate(@Nonnull final Object zeroValue,
      final BiFunction<Column, Column, Column> aggregator) {

    return vectorize(
        c -> functions.when(c.isNull(), zeroValue)
            .otherwise(functions.aggregate(c, functions.lit(zeroValue), aggregator::apply)),
        c -> functions.when(c.isNull(), zeroValue).otherwise(c)
    );
    // this is OK because aggregator(zero, x) == x
  }


  @Nonnull
  public ColumnCtx first() {
    return vectorize(c -> c.getItem(0), Function.identity());
  }


  public ColumnCtx last() {
    // we need to use `element_at()` here are `getItem()` does not support column arguments
    // NOTE: `element_at()` is 1-indexed as opposed to `getItem()` which is 0-indexed
    return vectorize(
        c -> functions.when(c.isNull().or(functions.size(c).equalTo(0)), null)
            .otherwise(functions.element_at(c, functions.size(c))),
        Function.identity()
    );
  }

  @Nonnull
  public ColumnCtx count() {
    return vectorize(
        c -> functions.when(c.isNull(), 0).otherwise(functions.size(c)),
        c -> functions.when(c.isNull(), 0).otherwise(1)
    );
  }

  @Nonnull
  public ColumnCtx empty() {
    return vectorize(
        c -> functions.when(c.isNotNull(), functions.size(c).equalTo(0)).otherwise(true),
        Column::isNull);
  }

  @Nonnull
  public ColumnCtx explode() {
    return andThen(new ColumnPaths.Explode(false));
  }

  @Nonnull
  public ColumnCtx explode_outer() {
    return andThen(new ColumnPaths.Explode(true));
  }

  @Nonnull
  public ColumnCtx resolve(@Nonnull final String separator) {
    return vectorize(c -> functions.array_join(c, separator), Function.identity());
  }

  @Nonnull
  public ColumnCtx not() {
    return transform(functions::not);
  }

  @Nonnull
  public ColumnCtx sum() {
    return aggregate(0, Column::plus);
  }


  @Nonnull
  public ColumnCtx max() {
    return vectorize(functions::array_max, Function.identity());
  }

  @Nonnull
  public ColumnCtx min() {
    return vectorize(functions::array_min, Function.identity());
  }

  @Nonnull
  public ColumnCtx allTrue() {
    return min().orElse(true);
  }

  @Nonnull
  public ColumnCtx allFalse() {
    return max().not().orElse(true);
  }

  @Nonnull
  public ColumnCtx anyTrue() {
    return max().orElse(false);
  }

  @Nonnull
  public ColumnCtx anyFalse() {
    return min().not().orElse(false);
  }

  /**
   * Call udf with this column as the first argument.
   */
  @Nonnull
  public ColumnCtx mapWithUDF(@Nonnull final String udfName, @Nonnull final ColumnCtx... args) {
    return transform(c -> functions.callUDF(udfName,
        Stream.concat(Stream.of(c), Stream.of(args).map(ColumnCtx::getValue))
            .toArray(Column[]::new)));

  }


  @Nonnull
  public ColumnCtx callUDF(@Nonnull final String udfName, @Nonnull final ColumnCtx... args) {
    return of(new ColumnPaths.CallUDF(udfName, args));
  }

  // @Nonnull
  // public ColumnCtx callUDF(@Nonnull final String udfName, @Nonnull final ColumnCtx... args) {
  //   return copyOf(functions.callUDF(udfName,
  //       Stream.concat(Stream.of(getValue()), Stream.of(args).map(ColumnCtx::getValue))
  //           .toArray(Column[]::new)));
  // }
  //
  // public StdColumnCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression,
  //     @Nonnull final Function<Column, Column> singularExpression) {
  //   return StdColumnCtx.of(
  //       ValueFunctions.ifArray(value, arrayExpression::apply, singularExpression::apply));
  // }
  //
  // @Override
  // @Nonnull
  // public StdColumnCtx getField(@Nonnull final String fieldName) {
  //   return of(ValueFunctions.unnest(value.getField(fieldName)));
  // }
  //
  // @Nonnull
  // public One<ColumnCtx> explode() {
  //   //  TODO: this actually cannot should return DatasetResult as filtering is required here
  //   final ColumnCtx exploded = vectorize(functions::explode, Function.identity());
  //   final String materializedColumnName = randomAlias();
  //   return DatasetResult.one(copyOf(functions.col(materializedColumnName)),
  //       ds -> ds.withColumn(
  //               materializedColumnName, exploded.getValue())
  //           .filter(functions.col(materializedColumnName).isNotNull()));
  // }
  //
  // @Nonnull
  // public One<ColumnCtx> explode_outer() {
  //   //  TODO: this actually cannot should return DatasetResult as filtering is required here
  //   final ColumnCtx exploded = vectorize(functions::explode_outer, Function.identity());
  //   final String materializedColumnName = randomAlias();
  //   return DatasetResult.one(copyOf(functions.col(materializedColumnName)),
  //       ds -> ds.withColumn(
  //           materializedColumnName, exploded.getValue()));
  // }
}

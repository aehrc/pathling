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
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;


/**
 * Describes the representation of a {@link au.csiro.pathling.fhirpath.collection.Collection} within
 * a {@link org.apache.spark.sql.Dataset}.
 * <p>
 * Also provides operations for transforming and manipulating the representation.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public abstract class ColumnRepresentation {

  /**
   * Create a new {@link ColumnRepresentation} from a literal value.
   *
   * @param value The value to represent
   * @return A new {@link ColumnRepresentation} representing the value
   */
  @Nonnull
  public static ColumnRepresentation literal(@Nonnull final Object value) {
    return new ArrayOrSingularRepresentation(functions.lit(value));
  }

  /**
   * Create a new {@link ColumnRepresentation} from the result of a function that takes two
   * {@link Column} operands and returns a single {@link Column} result.
   *
   * @param left The left operand
   * @param right The right operand
   * @param operator The function to apply to the operands
   * @return A new {@link ColumnRepresentation} representing the result of the function
   */
  @Nonnull
  public static ColumnRepresentation binaryOperator(@Nonnull final ColumnRepresentation left,
      @Nonnull final ColumnRepresentation right,
      @Nonnull final BiFunction<Column, Column, Column> operator) {
    return new ArrayOrSingularRepresentation(operator.apply(left.getValue(), right.getValue()));
  }

  /**
   * Create a new {@link ColumnRepresentation} from the result of a function that takes the column
   * of this representation and returns a new {@link Column}.
   *
   * @param function The function to apply to the column
   * @return A new {@link ColumnRepresentation} containing the result of the function
   */
  @Nonnull
  public ColumnRepresentation call(@Nonnull final Function<Column, Column> function) {
    return copyOf(function.apply(getValue()));
  }

  /**
   * Get the underlying {@link Column} that this representation is based on.
   *
   * @return The underlying {@link Column}
   */
  public abstract Column getValue();

  /**
   * Create a new {@link ColumnRepresentation} from a new {@link Column}.
   *
   * @param newValue The new {@link Column} to represent
   * @return A new {@link ColumnRepresentation} representing the new column
   */
  protected abstract ColumnRepresentation copyOf(@Nonnull final Column newValue);

  /**
   * Create a new {@link ColumnRepresentation} from the result of a function that takes an array
   * {@link Column} and returns a new array {@link Column}.
   */
  @Nonnull
  public ColumnRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression) {
    // The default implementation wraps a singular value in an array if it is not null.
    return vectorize(arrayExpression,
        c -> arrayExpression.apply(functions.when(c.isNotNull(), functions.array(c))));
  }

  /**
   * Create a new {@link ColumnRepresentation} by providing two functions: one that takes an array
   * and one that takes a singular value. The array function is applied to the column if it is an
   * array, and the singular function is applied to the column if it is not an array.
   *
   * @param arrayExpression The function to apply to the column if it is an array
   * @param singularExpression The function to apply to the column if it is not an array
   * @return A new {@link ColumnRepresentation} representing the result of the function
   */
  @Nonnull
  public abstract ColumnRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression);

  /**
   * Flattens the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is flattened
   */
  @Nonnull
  public abstract ColumnRepresentation flatten();

  /**
   * Returns a new {@link ColumnRepresentation} that represents the result of traversing to a nested
   * field within the current representation.
   *
   * @param fieldName The name of the field to traverse to
   * @return A new {@link ColumnRepresentation} representing the result of the traversal
   */
  @Nonnull
  public abstract ColumnRepresentation traverse(@Nonnull final String fieldName);

  /**
   * Returns a new {@link ColumnRepresentation} that represents the result of traversing to a nested
   * field within the current representation. This method also takes the FHIR type of the field into
   * account to return a more specific representation.
   *
   * @param fieldName The name of the field to traverse to
   * @param fhirType The FHIR type of the field
   * @return A new {@link ColumnRepresentation} representing the result of the traversal
   */
  @Nonnull
  public abstract ColumnRepresentation traverse(@Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType);

  /**
   * Converts the current {@link ColumnRepresentation} to a string value.
   *
   * @return An optional string value of the current {@link ColumnRepresentation}
   */
  public Optional<String> asStringValue() {
    return Optional.of(getValue().expr())
        .flatMap(maybeCast(Literal.class))
        .map(Literal::toString);
  }

  /**
   * Converts the current {@link ColumnRepresentation} to an array.
   *
   * @return A new {@link ColumnRepresentation} that is an array
   */
  @Nonnull
  public ColumnRepresentation toArray() {
    return vectorize(
        Function.identity(),
        c -> functions.when(c.isNotNull(), functions.array(c))
    );
  }

  /**
   * Combines the current {@link ColumnRepresentation} with another one.
   *
   * @param other The other {@link ColumnRepresentation} to combine with
   * @return A new {@link ColumnRepresentation} that is a combination of the current and the other
   * one
   */
  @Nonnull
  public ColumnRepresentation combine(@Nonnull final ColumnRepresentation other) {
    return copyOf(functions.concat(toArray().getValue(), other.toArray().getValue()));
  }

  /**
   * Returns the current {@link ColumnRepresentation} or a literal value if the current one is
   * null.
   *
   * @param value The literal value to return if the current {@link ColumnRepresentation} is null
   * @return The current {@link ColumnRepresentation} or the literal value if the current one is
   * null
   */
  @Nonnull
  public ColumnRepresentation orElse(@Nonnull final Object value) {
    return copyOf(functions.coalesce(getValue(), functions.lit(value)));
  }

  /**
   * Returns a singular value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is a singular value
   */
  @Nonnull
  public ColumnRepresentation singular() {
    return vectorize(
        c -> functions.when(functions.size(c).leq(1), c.getItem(0))
            .otherwise(functions.raise_error(
                functions.lit("Expected a single value, but found multiple values"))),
        Function.identity()
    );
  }

  /**
   * Filters the current {@link ColumnRepresentation} using a lambda function.
   *
   * @param lambda The lambda function to use for filtering
   * @return A new {@link ColumnRepresentation} that is filtered
   */
  @Nonnull
  public ColumnRepresentation filter(@Nonnull final Function<Column, Column> lambda) {
    return vectorize(
        c -> functions.filter(c, lambda::apply),
        c -> functions.when(c.isNotNull(), functions.when(lambda.apply(c), c))
    );
  }

  /**
   * Checks if the current {@link ColumnRepresentation} matches a regular expression.
   *
   * @param regex The regular expression to match against
   * @return A new {@link ColumnRepresentation} that is the result of the match
   */
  @Nonnull
  public ColumnRepresentation like(@Nonnull final String regex) {
    return copyOf(getValue().rlike(regex));
  }

  /**
   * Removes nulls from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that has no nulls
   */
  @Nonnull
  public ColumnRepresentation removeNulls() {
    return vectorize(
        c -> functions.filter(c, Column::isNotNull),
        Function.identity()
    );
  }

  /**
   * Transforms the current {@link ColumnRepresentation} using a lambda function.
   *
   * @param lambda The lambda function to use for transformation
   * @return A new {@link ColumnRepresentation} that is transformed
   */
  @Nonnull
  public ColumnRepresentation transform(final Function<Column, Column> lambda) {
    // TODO: Can this method and vectorize be rationalised?
    return vectorize(
        c -> functions.transform(c, lambda::apply),
        c -> functions.when(c.isNotNull(), lambda.apply(c))
    );
  }

  /**
   * Aggregates the current {@link ColumnRepresentation} using a zero value and an aggregator
   * function.
   *
   * @param zeroValue The zero value to use for aggregation
   * @param aggregator The aggregator function to use for aggregation
   * @return A new {@link ColumnRepresentation} that is aggregated
   */
  @Nonnull
  public ColumnRepresentation aggregate(@Nonnull final Object zeroValue,
      final BiFunction<Column, Column, Column> aggregator) {

    return vectorize(
        c -> functions.when(c.isNull(), zeroValue)
            .otherwise(functions.aggregate(c, functions.lit(zeroValue), aggregator::apply)),
        c -> functions.when(c.isNull(), zeroValue).otherwise(c)
    );
    // This is OK because: aggregator(zero, x) == x
  }

  /**
   * Returns the first value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the first value
   */
  @Nonnull
  public ColumnRepresentation first() {

    return vectorize(c -> c.getItem(0), Function.identity());
  }

  /**
   * Returns the last value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the last value
   */
  public ColumnRepresentation last() {
    // we need to use `element_at()` here are `getItem()` does not support column arguments
    // NOTE: `element_at()` is 1-indexed as opposed to `getItem()` which is 0-indexed
    return vectorize(
        c -> functions.when(c.isNull().or(functions.size(c).equalTo(0)), null)
            .otherwise(functions.element_at(c, functions.size(c))),
        Function.identity()
    );
  }

  /**
   * Counts the values in the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the count of values
   */
  @Nonnull
  public ColumnRepresentation count() {
    return vectorize(
        c -> functions.when(c.isNull(), 0).otherwise(functions.size(c)),
        c -> functions.when(c.isNull(), 0).otherwise(1)
    );
  }

  /**
   * Checks if the current {@link ColumnRepresentation} is empty.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation empty() {
    return vectorize(
        c -> functions.when(c.isNotNull(), functions.size(c).equalTo(0)).otherwise(true),
        Column::isNull);
  }

  /**
   * Joins the current {@link ColumnRepresentation} with a separator.
   *
   * @param separator The separator to use for joining
   * @return A new {@link ColumnRepresentation} that is joined
   */
  @Nonnull
  public ColumnRepresentation join(@Nonnull final ColumnRepresentation separator) {
    return vectorize(c -> new Column(new ArrayJoin(c.expr(), separator.getValue().expr())),
        Function.identity());
  }

  /**
   * Negates the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is negated
   */
  @Nonnull
  public ColumnRepresentation not() {
    return transform(functions::not);
  }

  /**
   * Sums the values in the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the sum of values
   */
  @Nonnull
  public ColumnRepresentation sum() {
    return aggregate(0, Column::plus);
  }

  /**
   * Returns the maximum value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the maximum value
   */
  @Nonnull
  public ColumnRepresentation max() {
    return vectorize(functions::array_max, Function.identity());
  }

  /**
   * Returns the minimum value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the minimum value
   */
  @Nonnull
  public ColumnRepresentation min() {
    return vectorize(functions::array_min, Function.identity());
  }

  /**
   * Checks if all values in the current {@link ColumnRepresentation} are true.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation allTrue() {
    return min().orElse(true);
  }

  /**
   * Checks if all values in the current {@link ColumnRepresentation} are false.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation allFalse() {
    return max().not().orElse(true);
  }

  /**
   * Checks if any value in the current {@link ColumnRepresentation} is true.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation anyTrue() {
    return max().orElse(false);
  }

  /**
   * Checks if any value in the current {@link ColumnRepresentation} is false.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation anyFalse() {
    return min().not().orElse(false);
  }

  /**
   * Calls a UDF with the current {@link ColumnRepresentation} and other arguments, and uses the
   * result to transform the current {@link ColumnRepresentation}.
   *
   * @param udfName The name of the UDF to call
   * @param args The arguments to pass to the UDF
   * @return A new {@link ColumnRepresentation} that is the result of the UDF call
   */
  @Nonnull
  public ColumnRepresentation transformWithUdf(@Nonnull final String udfName,
      @Nonnull final ColumnRepresentation... args) {
    return transform(c -> functions.callUDF(udfName,
        Stream.concat(Stream.of(c), Stream.of(args).map(ColumnRepresentation::getValue))
            .toArray(Column[]::new)));

  }

  /**
   * Calls a UDF with the current {@link ColumnRepresentation} and other arguments, and returns the
   * result as a new {@link ColumnRepresentation}.
   *
   * @param udfName The name of the UDF to call
   * @param args The arguments to pass to the UDF
   * @return A new {@link ColumnRepresentation} that is the result of the UDF call
   */
  @Nonnull
  public ColumnRepresentation callUdf(@Nonnull final String udfName,
      @Nonnull final ColumnRepresentation... args) {
    return copyOf(functions.callUDF(udfName,
        Stream.concat(Stream.of(getValue()), Stream.of(args).map(ColumnRepresentation::getValue))
            .toArray(Column[]::new)));
  }

  /**
   * Casts the current {@link ColumnRepresentation} to a different data type.
   *
   * @param dataType The data type to cast to
   * @return A new {@link ColumnRepresentation} that is the result of the cast
   */
  @Nonnull
  public ColumnRepresentation cast(@Nonnull final DataType dataType) {
    return copyOf(getValue().cast(dataType));
  }

  /**
   * Casts the current {@link ColumnRepresentation} to a string.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the cast
   */
  @Nonnull
  public ColumnRepresentation asString() {
    return cast(DataTypes.StringType);
  }

  /**
   * Explodes the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is exploded
   */
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

  /**
   * Explodes the current {@link ColumnRepresentation} with outer join.
   *
   * @return A new {@link ColumnRepresentation} that is exploded with outer join
   */
  @Nonnull
  public One<ColumnRepresentation> explodeOuter() {
    //  TODO: this actually cannot should return DatasetResult as filtering is required here
    final ColumnRepresentation exploded = vectorize(functions::explode_outer, Function.identity());
    final String materializedColumnName = randomAlias();
    return DatasetResult.one(copyOf(functions.col(materializedColumnName)),
        ds -> ds.withColumn(
            materializedColumnName, exploded.getValue()));
  }

}
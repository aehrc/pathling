/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import static java.util.Objects.nonNull;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.raise_error;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Column$;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import scala.Predef;


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
   * Wrapper on the Spark SQL functions object to allow easier access to functions in Java.
   *
   * @param arrayColumn the array column
   * @param index the index
   * @return the column at the specified index
   */
  @Nonnull
  public static Column getAt(@Nonnull final Column arrayColumn, int index) {
    return functions.get(arrayColumn, lit(index));
  }

  /**
   * Error message used when expecting a singular collection but finding multiple elements.
   */
  public static final String DEF_NOT_SINGULAR_ERROR = "Expecting a collection with a single element but it has many.";

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
      @Nonnull final BinaryOperator<Column> operator) {
    return new DefaultRepresentation(operator.apply(left.getValue(), right.getValue()));
  }

  /**
   * Create a new {@link ColumnRepresentation} from the result of a function that takes the column
   * of this representation and returns a new {@link Column}.
   *
   * @param function The function to apply to the column
   * @return A new {@link ColumnRepresentation} containing the result of the function
   */
  @Nonnull
  public ColumnRepresentation call(@Nonnull final UnaryOperator<Column> function) {
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
   * Maps the current {@link ColumnRepresentation} using a lambda function.
   *
   * @param lambda The lambda function to apply to the column
   * @return A new {@link ColumnRepresentation} containing the result of the function
   */
  @Nonnull
  public ColumnRepresentation map(@Nonnull final UnaryOperator<Column> lambda) {
    return copyOf(lambda.apply(getValue()));
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
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> singularExpression);

  /**
   * Flattens the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is flattened
   */
  @Nonnull
  public abstract ColumnRepresentation flatten();

  /**
   * Returns a new {@link ColumnRepresentation} that represents the result of traversing to a nested
   * field within the current representation. The result is flattened.
   *
   * @param fieldName The name of the field to traverse to
   * @return A new {@link ColumnRepresentation} representing the result of the traversal
   */
  @Nonnull
  public abstract ColumnRepresentation traverse(@Nonnull final String fieldName);

  /**
   * Returns a new {@link ColumnRepresentation} that represents the result of traversing to a nested
   * field within the current representation. The results can be nested.
   *
   * @param fieldName The name of the field to traverse to
   * @return A new {@link ColumnRepresentation} representing the result of the traversal
   */
  @Nonnull
  public abstract ColumnRepresentation getField(@Nonnull final String fieldName);

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
    return Optional.of(getValue().node())
        .flatMap(maybeCast(Literal.class))
        .map(Literal::value)
        .map(Object::toString);
  }

  /**
   * Converts the current {@link ColumnRepresentation} to an array.
   *
   * @return A new {@link ColumnRepresentation} that is an array
   */
  @Nonnull
  public ColumnRepresentation toArray() {
    return vectorize(
        UnaryOperator.identity(),
        c -> when(c.isNotNull(), array(c))
    );
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
    return copyOf(coalesce(getValue(), lit(value)));
  }


  /**
   * Returns a singular value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is a singular value
   */
  @Nonnull
  public ColumnRepresentation singular() {
    return singular(null);
  }


  /**
   * Returns a singular value from the current {@link ColumnRepresentation}.
   *
   * @param errorMessage the error message to use when the representation cannot be singularized.
   * @return A new {@link ColumnRepresentation} that is a singular value
   */
  @Nonnull
  public ColumnRepresentation singular(@Nullable final String errorMessage) {
    return vectorize(
        c -> when(c.isNull().or(size(c).leq(1)), getAt(c, 0))
            .otherwise(raise_error(lit(nonNull(errorMessage)
                                       ? errorMessage
                                       : DEF_NOT_SINGULAR_ERROR))),
        UnaryOperator.identity()
    );
  }

  /**
   * Converts the current {@link ColumnRepresentation} a plural representation. The values are
   * represented as arrays where an empty collection is represented as an empty array. This is
   * different from the {@link ColumnRepresentation#toArray()} method which represents an empty
   * collection as null.
   *
   * @return A new {@link ColumnRepresentation} that is an array
   */
  @Nonnull
  public ColumnRepresentation plural() {
    return vectorize(
        a -> when(a.isNotNull(), a).otherwise(array()),
        c -> when(c.isNotNull(), array(c)).otherwise(array())
    );
  }


  /**
   * Applies a mapping column to this column representation.
   *
   * @param mapColumn the mapping column to apply
   * @return a new ColumnRepresentation with the mapping applied
   */
  @Nonnull
  public ColumnRepresentation applyTo(@Nonnull final Column mapColumn) {
    return transform(mapColumn::apply).removeNulls().flatten();
  }

  /**
   * Filters the current {@link ColumnRepresentation} using a lambda function.
   *
   * @param lambda The lambda function to use for filtering
   * @return A new {@link ColumnRepresentation} that is filtered
   */
  @Nonnull
  public ColumnRepresentation filter(@Nonnull final UnaryOperator<Column> lambda) {
    return vectorize(
        c -> functions.filter(c, lambda::apply),
        c -> when(c.isNotNull(), when(lambda.apply(c), c))
    );
  }

  /**
   * Inverts the current {@link ColumnRepresentation} (applies unary minus).
   *
   * @return A new {@link ColumnRepresentation} that is inverted
   */
  @Nonnull
  public ColumnRepresentation negate() {
    return map(Column::unary_$minus);
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
        UnaryOperator.identity()
    );
  }

  /**
   * Converts empty arrays to nulls in the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} when all empty collections are represented as NULLs
   * regardless of their arity.
   */
  @Nonnull
  public ColumnRepresentation normaliseNull() {
    return vectorize(
        c -> when(c.isNull().or(size(c).equalTo(0)), null).otherwise(c),
        UnaryOperator.identity()
    );
  }

  /**
   * Converts the current {@link ColumnRepresentation} to a canonical form.
   *
   * @return a canonical form of this column representation
   */
  @Nonnull
  public ColumnRepresentation asCanonical() {
    return removeNulls().normaliseNull();
  }

  /**
   * Transforms the current {@link ColumnRepresentation} in a way that only affects a singular value
   * if it is not null.
   *
   * @param lambda The lambda function to use for transformation
   * @return A new {@link ColumnRepresentation} that is transformed
   */
  @Nonnull
  public ColumnRepresentation transform(final UnaryOperator<Column> lambda) {
    return vectorize(
        c -> functions.transform(c, lambda::apply),
        c -> when(c.isNotNull(), lambda.apply(c))
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
      final BinaryOperator<Column> aggregator) {

    return vectorize(
        c -> when(c.isNull(), zeroValue)
            .otherwise(functions.aggregate(c, lit(zeroValue), aggregator::apply)),
        c -> when(c.isNull(), zeroValue).otherwise(c)
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

    return vectorize(a -> getAt(a, 0), UnaryOperator.identity());
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
        c -> when(c.isNull().or(size(c).equalTo(0)), null)
            .otherwise(element_at(c, size(c))),
        UnaryOperator.identity()
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
        c -> when(c.isNull(), 0).otherwise(size(c)),
        c -> when(c.isNull(), 0).otherwise(1)
    );
  }


  /**
   * Checks if the current {@link ColumnRepresentation} is empty.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation isEmpty() {
    return vectorize(
        c -> when(c.isNotNull(), size(c).equalTo(0)).otherwise(true),
        Column::isNull);
  }


  /**
   * Gets the boolean value of the current {@link ColumnRepresentation}. Returns `true` for
   * non-empty representations or `null` otherwise. This is NOT a simple type cast!
   *
   * @return A new {@link ColumnRepresentation} with the boolean representation.
   */
  @Nonnull
  public ColumnRepresentation toBoolean() {
    return vectorize(
        a -> when(size(a).notEqual(0), lit(true)),
        s -> when(s.isNotNull(), lit(true)));
  }


  /**
   * Joins the current {@link ColumnRepresentation} with a separator.
   *
   * @param separator The separator to use for joining
   * @return A new {@link ColumnRepresentation} that is joined
   */
  @Nonnull
  public ColumnRepresentation join(@Nonnull final ColumnRepresentation separator) {
    // NOTE: We must call the Scala companion object `Column$.MODULE$.fn` rather than `Column.fn`
    // because the Scala varargs method `def fn(name: String, cols: Column*)` is not directly
    // accessible from Java. The Scala compiler normally expands `cols*` into a Seq[Column],
    // but Java cannot perform that expansion automatically. To replicate it, we use
    // `Predef.wrapRefArray(...)` to convert the Java array into a Scala `ArraySeq`, then call
    // `.toSeq()` to obtain the immutable `Seq[Column]` expected by the Scala method.
    return vectorize(c -> Column$.MODULE$.fn("array_join",
            Predef.wrapRefArray(
                new Column[]{getValue(), separator.getValue()}).toSeq()),
        UnaryOperator.identity());
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
    return vectorize(functions::array_max, UnaryOperator.identity());
  }

  /**
   * Returns the minimum value from the current {@link ColumnRepresentation}.
   *
   * @return A new {@link ColumnRepresentation} that is the minimum value
   */
  @Nonnull
  public ColumnRepresentation min() {
    return vectorize(functions::array_min, UnaryOperator.identity());
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
    return transform(c -> callUDF(udfName,
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
    return copyOf(callUDF(udfName,
        Stream.concat(Stream.of(getValue()), Stream.of(args).map(ColumnRepresentation::getValue))
            .toArray(Column[]::new)));
  }

  /**
   * Casts (elementwise) the current {@link ColumnRepresentation} to a different data type.
   *
   * @param dataType The data type to cast to
   * @return A new {@link ColumnRepresentation} that is the result of the cast
   */
  @Nonnull
  public ColumnRepresentation elementCast(@Nonnull final DataType dataType) {
    return vectorize(
        a -> a.cast(DataTypes.createArrayType(dataType)),
        s -> s.cast(dataType)
    );
  }

  /**
   * Casts the current {@link ColumnRepresentation} to a string.
   *
   * @return A new {@link ColumnRepresentation} that is the result of the cast
   */
  @Nonnull
  public ColumnRepresentation asString() {
    return elementCast(DataTypes.StringType);
  }


  /**
   * Checks if the current {@link ColumnRepresentation} contains to another one using a specified
   * comparator. If the tested element is NULL the result is also NULL. If the tested collection is
   * NULL the result is false.
   *
   * @param element The element to check for
   * @param comparator The comparator to use
   * @return A new {@link ColumnRepresentation} that is the result of the check
   */
  @Nonnull
  public ColumnRepresentation contains(@Nonnull final ColumnRepresentation element,
      @Nonnull final BinaryOperator<Column> comparator) {
    return vectorize(
        a -> when(element.getValue().isNotNull(),
            coalesce(exists(a, e -> comparator.apply(e, element.getValue())),
                lit(false))),
        c -> when(element.getValue().isNotNull(),
            coalesce(comparator.apply(c, element.getValue()), lit(false)))
    );
  }

  /**
   * Traverses the current {@link ColumnRepresentation} to a selected elements in a choice and
   * returns a new {@link ColumnRepresentation} that is the result of the traversal.
   *
   * @param definitions The definitions to traverse to
   * @return A new {@link ColumnRepresentation} that is the result of the traversal
   */
  @Nonnull
  public ColumnRepresentation traverseChoice(@Nonnull final ElementDefinition... definitions) {
    return transform(c -> coalesce(Stream.of(definitions)
        .map(
            ed -> this.copyOf(c)
                .traverse(ed.getElementName(), ed.getFhirType())
                .getValue()
        ).toArray(Column[]::new)));
  }
}

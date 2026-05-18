/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import static org.apache.spark.sql.functions.aggregate;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.ifnull;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.when;

import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.classic.ColumnConversions$;
import org.apache.spark.sql.functions;

/**
 * Pathling-specific SQL functions that extend Spark SQL functionality.
 *
 * <p>Provides utilities for working with Spark SQL columns in the context of FHIR data processing,
 * including FHIR-instant formatting, array deduplication with custom equality semantics, and
 * let-binding for safe evaluation of non-deterministic column expressions.
 */
@UtilityClass
public class SqlFunctions {

  private static final String FHIR_INSTANT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

  /**
   * Formats a TIMESTAMP column to a string in FHIR instant format. Always returns UTC time as Spark
   * TIMESTAMP does not preserve the original timezone.
   *
   * @param col The column containing TIMESTAMP values to format
   * @return A new column with TIMESTAMP values formatted as strings in FHIR instant format
   */
  @Nonnull
  public static Column toFhirInstant(@Nonnull final Column col) {
    return functions.date_format(
        functions.to_utc_timestamp(col, functions.current_timezone()), FHIR_INSTANT_FORMAT);
  }

  /**
   * Deduplicates an array using custom equality comparator. Implements manual deduplication with
   * aggregate() for types requiring custom equality semantics (Quantity, Coding, temporal types).
   *
   * @param arrayColumn the array column to deduplicate
   * @param equalityComparator a function that compares two elements for equality
   * @return deduplicated array column
   */
  @Nonnull
  public static Column arrayDistinctWithEquality(
      @Nonnull final Column arrayColumn, @Nonnull final BinaryOperator<Column> equalityComparator) {
    return let(
        arrayColumn,
        ac -> {
          final Column emptyTypedArray = filter(ac, x -> lit(false));
          return aggregate(
              ac,
              emptyTypedArray,
              (acc, elem) ->
                  when(
                          not(
                              exists(
                                  acc, x -> ifnull(equalityComparator.apply(x, elem), lit(false)))),
                          concat(acc, array(elem)))
                      .otherwise(acc));
        });
  }

  /**
   * Merges and deduplicates two arrays using custom equality comparator. Concatenates the arrays
   * and then deduplicates using the provided equality function.
   *
   * @param leftArray the left array column
   * @param rightArray the right array column
   * @param equalityComparator a function that compares two elements for equality
   * @return merged and deduplicated array column
   */
  @Nonnull
  public static Column arrayUnionWithEquality(
      @Nonnull final Column leftArray,
      @Nonnull final Column rightArray,
      @Nonnull final BinaryOperator<Column> equalityComparator) {

    final Column combined = concat(leftArray, rightArray);
    return arrayDistinctWithEquality(combined, equalityComparator);
  }

  /**
   * Evaluates {@code value} exactly once per row and passes the result to {@code body}.
   *
   * <p>This matters for {@link org.apache.spark.sql.catalyst.expressions.Nondeterministic} operands
   * such as {@link TraceExpression}: without materialisation, each reference to the same
   * non-deterministic expression in a Spark tree evaluates independently, firing side effects
   * multiple times.
   *
   * <p>For deterministic {@code value}, returns {@code body.apply(value)} directly, incurring no
   * HOF overhead. For non-deterministic {@code value}, uses {@code
   * element_at(transform(array(value), body), 1)} to materialise the operand once via {@code array}
   * before the lambda runs.
   *
   * <p>The result is {@code Nondeterministic} if and only if {@code value} or the expression
   * returned by {@code body} is.
   *
   * <p>The resulting expression has no logical-plan dependency and composes inside any relational
   * context (select, filter, join, window). Unlike Spark Catalyst's {@code With} expression, it
   * does not rewrite into a {@code Project} operator.
   *
   * <p><strong>Constraint.</strong> When {@code value} is non-deterministic, it MUST NOT contain a
   * SQL aggregate or window expression; Spark's analyzer rejects these inside higher-order function
   * arguments.
   *
   * @param value the operand to evaluate once per row
   * @param body the lambda that consumes the evaluated operand
   * @return a column expression applying {@code body} to a single evaluation of {@code value}
   */
  @Nonnull
  public static Column let(@Nonnull final Column value, @Nonnull final UnaryOperator<Column> body) {
    // Deterministic expressions need no materialisation: identical references in the tree always
    // produce the same value, so single-fire is trivially satisfied. The HOF wrapper is reserved
    // for non-deterministic operands (e.g. TraceExpression) that fire side effects on every
    // tree reference.
    //
    // ColumnConversions$.MODULE$.expression() is used instead of ExpressionUtils.expression()
    // because ExpressionUtils can return a surrogate expression (e.g. when the Column wraps a
    // compound expression like concat or coalesce whose children include a Nondeterministic node)
    // that reports deterministic() = true even though the full expression tree contains a
    // non-deterministic sub-expression. ColumnConversions$.MODULE$.expression() always returns the
    // real underlying Catalyst Expression, preserving the correct determinism semantics through
    // the entire tree.
    if (ColumnConversions$.MODULE$.expression(value).deterministic()) {
      return body.apply(value);
    }
    return element_at(transform(array(value), body::apply), 1);
  }
}

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

package au.csiro.pathling.fhirpath.comparison;

import static au.csiro.pathling.schema.ColumnBinding.bindOnce;

import au.csiro.pathling.fhirpath.column.SystemDateTime;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.sql.misc.HighBoundaryForTime;
import au.csiro.pathling.sql.misc.LowBoundaryForTime;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Comparator for Temporal (DateTime and Time) values that handles partial dates by comparing
 * ranges.
 *
 * <p>Since FHIR DateTime/Time values can be partial (e.g., just year or year-month), comparisons
 * need to consider the range of possible values for each DateTime/Time. The range of a value is a
 * structure of its lower bound, {@code __start}, and its upper bound, {@code __end}, and is
 * computed once for each operand however many times the comparison reads its bounds.
 */
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class TemporalComparator implements ColumnComparator, ElementWiseEquality {

  @Nonnull private static final String START = LayoutFields.START_ANNOTATION;

  @Nonnull private static final String END = LayoutFields.END_ANNOTATION;

  @Nonnull
  private static final TemporalComparator DATE_TIME_COMPARATOR =
      new TemporalComparator(SystemDateTime::range);

  @Nonnull
  private static final TemporalComparator TIME_COMPARATOR =
      new TemporalComparator(
          time ->
              functions.struct(
                  functions.callUDF(LowBoundaryForTime.FUNCTION_NAME, time).alias(START),
                  functions.callUDF(HighBoundaryForTime.FUNCTION_NAME, time).alias(END)));

  /** Takes an operand to its range. */
  @Nonnull private final UnaryOperator<Column> rangeOf;

  /**
   * Returns a TemporalComparator for System Date and DateTime values, which carry their ranges.
   *
   * @return A TemporalComparator that can compare DateTime values.
   */
  @Nonnull
  public static TemporalComparator forDateTime() {
    return DATE_TIME_COMPARATOR;
  }

  /**
   * Returns a TemporalComparator for Time values.
   *
   * @return A TemporalComparator that can compare Time values.
   */
  @Nonnull
  public static TemporalComparator forTime() {
    return TIME_COMPARATOR;
  }

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::equalTo);
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::lt);
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::leq);
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::gt);
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::geq);
  }

  @Nonnull
  private Column implementWithSql(
      @Nonnull final Column left,
      @Nonnull final Column right,
      @Nonnull final BinaryOperator<Column> comparator) {
    // Each range is evaluated once, so a range computed from text parses it once for both bounds.
    return bindOnce(
        rangeOf.apply(left),
        leftRange ->
            bindOnce(
                rangeOf.apply(right),
                rightRange ->
                    // If canCompare apply the comparator to the low bound (either one is fine),
                    // else return null.
                    functions
                        .when(
                            canCompare(leftRange, rightRange),
                            comparator.apply(leftRange.getField(START), rightRange.getField(START)))
                        .otherwise(functions.lit(null))));
  }

  @Nonnull
  private static Column canCompare(@Nonnull final Column left, @Nonnull final Column right) {

    // can compare when:
    // - either on overlapping
    // - or equal

    // This assumes that there is not case of partial overlaps, but I cannot see how that be
    // possible.
    // This is because:
    // 1. At any given precision, all the instants are disjointed (considering their lower and upper
    // bounds)
    // 2. Any more precise instant is fully contained within the less precise instant (considering
    // their lower and upper bounds)
    return left.getField(END)
        .lt(right.getField(START))
        .or(right.getField(END).lt(left.getField(START)))
        .or(
            right
                .getField(START)
                .equalTo(left.getField(START))
                .and(left.getField(END).equalTo(right.getField(END))));
  }
}

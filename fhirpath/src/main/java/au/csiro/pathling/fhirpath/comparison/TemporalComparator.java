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

import au.csiro.pathling.fhirpath.column.SystemDateTime;
import au.csiro.pathling.sql.SqlFunctions;
import au.csiro.pathling.sql.misc.HighBoundaryForTime;
import au.csiro.pathling.sql.misc.LowBoundaryForTime;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BiFunction;
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
 * need to consider the range of possible values for each DateTime/Time. The range of a value is its
 * lower bound and its upper bound, which the comparison reads several times.
 *
 * <p>A date's bounds are read through its accessors, {@link SystemDateTime#start} and {@link
 * SystemDateTime#end}: the stored annotation where the date carries it, and otherwise the bound
 * computed over a memoised parse, so a bound read more than once is parsed once (decision 145). A
 * time's bounds are each computed from it by a function of their own, wherever the comparison reads
 * them. Each operand, date or time, is bound with {@link SqlFunctions#let}, so that a traced
 * operand is evaluated once however many of its bounds are read (#2594).
 *
 * <p>A comparison is empty where the ranges overlap without being equal, so it is a {@code CASE}
 * that Parquet cannot push down. Beside it each comparison emits a plain conjunct on the bounds
 * that every true result implies, and that holds wherever the comparison is empty, because the
 * ranges then overlap (decision 144). The conjunction therefore has the comparison's value in every
 * row, under {@code not()} as elsewhere, and where a stored bound is compared with a literal Spark
 * pushes the plain conjunct to the scan and keeps the {@code CASE} as a residual filter.
 */
@AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class TemporalComparator implements ColumnComparator, ElementWiseEquality {

  @Nonnull
  private static final TemporalComparator DATE_TIME_COMPARATOR =
      new TemporalComparator(
          SystemDateTime::start, SystemDateTime::end, Optional.of(TemporalComparator::dateKey));

  @Nonnull
  private static final TemporalComparator TIME_COMPARATOR =
      new TemporalComparator(
          time -> functions.callUDF(LowBoundaryForTime.FUNCTION_NAME, time),
          time -> functions.callUDF(HighBoundaryForTime.FUNCTION_NAME, time),
          Optional.empty());

  /** The accessor of the lower bound of an operand. */
  @Nonnull private final UnaryOperator<Column> start;

  /** The accessor of the upper bound of an operand. */
  @Nonnull private final UnaryOperator<Column> end;

  /** The key by which equal values are deduplicated, where there is one. */
  @Nonnull private final Optional<UnaryOperator<Column>> deduplicationKey;

  /**
   * Returns a TemporalComparator for Date and DateTime values, whose bounds it reads through their
   * accessors.
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
  public Optional<UnaryOperator<Column>> deduplicationKey() {
    return deduplicationKey;
  }

  /**
   * The deduplication key of a date: two dates are equal exactly where their keys are (decision
   * 151).
   *
   * <p>Dates are equal exactly where their ranges are, because dates whose ranges differ either do
   * not overlap, and are unequal, or overlap, and cannot be compared, which is not equality. A date
   * without a time part has a range that its text alone decides, so its text is its key and nothing
   * is parsed. That relies on FHIR R4's rule that a date or dateTime without a time has no offset:
   * {@code date} forbids one, and {@code dateTime} allows one only after {@code Thh:mm:ss}. With no
   * offset, no two such texts have the same range. Stored data keeps the rule under the trust
   * contract (ticket 31); the engine's own date grammar is looser, and would give {@code
   * 2020-01-01Z} a key other than that of {@code 2020-01-01}, which it compares equal. Any other
   * date's key is its range, read through its accessors: an instant whatever its time zone, and a
   * value to the second the same as one to the millisecond, because both are exact. The text and
   * the range are separate fields of the key, exactly one of them set, so the two kinds never
   * collide. A date without a time part never has the range of one with a time part, so this loses
   * no equality.
   */
  @Nonnull
  private static Column dateKey(@Nonnull final Column date) {
    // The date is bound once, as the guard and the key both read it, so that a traced date is
    // evaluated once (#2594).
    return SqlFunctions.let(
        date,
        d ->
            functions.when(
                d.isNotNull(),
                functions.struct(
                    functions
                        .when(timed(d), functions.lit(null))
                        .otherwise(SystemDateTime.text(d))
                        .alias("text"),
                    functions.when(timed(d), SystemDateTime.start(d)).alias("start"),
                    functions.when(timed(d), SystemDateTime.end(d)).alias("end"))));
  }

  /** Whether a date has a time part. */
  @Nonnull
  private static Column timed(@Nonnull final Column date) {
    return SystemDateTime.text(date).contains("T");
  }

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    // Equal ranges overlap.
    return implementWithSql(
        left,
        right,
        Column::equalTo,
        (l, r) -> l.getStart().leq(r.getEnd()).and(l.getEnd().geq(r.getStart())));
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::lt, TemporalComparator::startsBeforeEnd);
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::leq, TemporalComparator::startsBeforeEnd);
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::gt, TemporalComparator::endsAfterStart);
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return implementWithSql(left, right, Column::geq, TemporalComparator::endsAfterStart);
  }

  /**
   * The conjunct implied by a left range that starts before, or at the same time as, the right one:
   * the left range starts no later than the right one ends.
   */
  @Nonnull
  private static Column startsBeforeEnd(
      @Nonnull final TemporalRange left, @Nonnull final TemporalRange right) {
    return left.getStart().leq(right.getEnd());
  }

  /**
   * The conjunct implied by a left range that starts after, or at the same time as, the right one:
   * the left range ends no earlier than the right one starts.
   */
  @Nonnull
  private static Column endsAfterStart(
      @Nonnull final TemporalRange left, @Nonnull final TemporalRange right) {
    return left.getEnd().geq(right.getStart());
  }

  @Nonnull
  private Column implementWithSql(
      @Nonnull final Column left,
      @Nonnull final Column right,
      @Nonnull final BinaryOperator<Column> comparator,
      @Nonnull final BiFunction<TemporalRange, TemporalRange, Column> superset) {
    return SqlFunctions.let(
        left,
        right,
        (l, r) -> {
          final TemporalRange leftRange = new TemporalRange(start.apply(l), end.apply(l));
          final TemporalRange rightRange = new TemporalRange(start.apply(r), end.apply(r));
          // If canCompare apply the comparator to the low bound (either one is fine), else return
          // null.
          return functions
              .when(
                  canCompare(leftRange, rightRange),
                  comparator.apply(leftRange.getStart(), rightRange.getStart()))
              .and(superset.apply(leftRange, rightRange));
        });
  }

  @Nonnull
  private static Column canCompare(
      @Nonnull final TemporalRange left, @Nonnull final TemporalRange right) {

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
    return left.getEnd()
        .lt(right.getStart())
        .or(right.getEnd().lt(left.getStart()))
        .or(right.getStart().equalTo(left.getStart()).and(left.getEnd().equalTo(right.getEnd())));
  }
}

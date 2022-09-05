/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Describes a path that can be compared with other paths, e.g. for equality.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public interface Comparable {

  /**
   * The inteface that defines comparison operation on columns. The actual implemenation and the
   * implemented operation depend on the type of value in the column.
   */
  public interface Comparator {

    Column equalsTo(Column left, Column right);

    default Column notEqual(Column left, Column right) {
      return functions.not(equalsTo(left, right));
    }

    Column lessThan(Column left, Column right);

    default Column lessThanOrEqual(final Column left, final Column right) {
      return lessThan(left, right).or(equalsTo(left, right));
    }

    Column greaterThan(Column left, Column right);

    default Column greaterThanOrEqual(final Column left, final Column right) {
      return greaterThan(left, right).or(equalsTo(left, right));
    }
  }

  /**
   * The implementation of comparator that use the standard Spark SQL operators.
   */
  class StandardComparator implements Comparator {

    @Override
    public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
      return left.equalTo(right);
    }

    @Override
    public Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
      return left.notEqual(right);
    }

    @Override
    public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
      return left.lt(right);
    }

    @Override
    public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
      return left.leq(right);
    }

    @Override
    public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
      return left.gt(right);
    }

    @Override
    public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
      return left.geq(right);
    }
  }

  Comparator STD_COMPARATOR = new StandardComparator();

  /**
   * Get a function that can take two Comparable paths and return a {@link Column} that contains a
   * comparison condition. The type of condition is controlled by supplying a {@link
   * ComparisonOperation}.
   *
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A {@link Function} that takes a Comparable as its parameter, and returns a {@link
   * Column}
   */
  @Nonnull
  Function<Comparable, Column> getComparison(@Nonnull ComparisonOperation operation);

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return A {@link Column}
   */
  @Nonnull
  Column getValueColumn();

  /**
   * @param type A subtype of {@link FhirPath}
   * @return {@code true} if this path can be compared to the specified class
   */
  boolean isComparableTo(@Nonnull Class<? extends Comparable> type);

  /**
   * Builds a comparison function for directly comparable paths using the custom comparator.
   *
   * @param source The path to build the comparison function for
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @param comparator The {@link Comparator} to use
   * @return A new {@link Function}
   */
  @Nonnull
  static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation, @Nonnull final Comparator comparator) {

    final TriFunction<Comparator, Column, Column, Column> compFunction = operation.compFunction;

    return target -> compFunction
        .apply(comparator, source.getValueColumn(), target.getValueColumn());
  }

  /**
   * Builds a comparison function for directly comparable paths using the standard Spark SQL
   * comparison operators.
   *
   * @param source The path to build the comparison function for
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A new {@link Function}
   */
  static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {

    return buildComparison(source, operation, STD_COMPARATOR);
  }

  /**
   * Represents a type of comparison operation.
   */
  enum ComparisonOperation {
    /**
     * The equals operation.
     */
    EQUALS("=", Comparator::equalsTo),

    /**
     * The not equals operation.
     */
    NOT_EQUALS("!=", Comparator::notEqual),

    /**
     * The less than or equal to operation.
     */
    LESS_THAN_OR_EQUAL_TO("<=", Comparator::lessThanOrEqual),

    /**
     * The less than operation.
     */
    LESS_THAN("<", Comparator::lessThan),

    /**
     * The greater than or equal to operation.
     */
    GREATER_THAN_OR_EQUAL_TO(">=", Comparator::greaterThanOrEqual),

    /**
     * The greater than operation.
     */
    GREATER_THAN(">", Comparator::greaterThan);

    @Nonnull
    private final String fhirPath;

    @Nonnull
    private final TriFunction<Comparator, Column, Column, Column> compFunction;

    ComparisonOperation(@Nonnull final String fhirPath,
        @Nonnull final TriFunction<Comparator, Column, Column, Column> compFunction) {
      this.fhirPath = fhirPath;
      this.compFunction = compFunction;
    }

    @Override
    @Nonnull
    public String toString() {
      return fhirPath;
    }
  }
}

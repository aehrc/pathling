/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Describes a path that is able to be compared with other paths, e.g. for equality.
 *
 * @author John Grimes
 */
public interface Comparable {

  /**
   * Get a function that can take two Comparable paths and return a {@link Column} that contains a
   * comparison condition. The type of condition is controlled by supplying a Spark column function,
   * e.g. {@link Column#equalTo}.
   *
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A {@link Function} that takes a Comparable as its parameter, and returns a {@link
   * Column}
   */
  Function<Comparable, Column> getComparison(ComparisonOperation operation);

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return A {@link Column}
   */
  Column getValueColumn();

  /**
   * @param type A subtype of {@link FhirPath}
   * @return {@code true} if this path can be compared to the specified class
   */
  boolean isComparableTo(@Nonnull Class<? extends Comparable> type);

  /**
   * Represents a type of comparison operation.
   */
  enum ComparisonOperation {
    /**
     * The equals operation.
     */
    EQUALS("=", Column::equalTo),

    /**
     * The not equals operation.
     */
    NOT_EQUALS("!=", Column::notEqual),

    /**
     * The less than or equal to operation.
     */
    LESS_THAN_OR_EQUAL_TO("<=", Column::leq),

    /**
     * The less than operation.
     */
    LESS_THAN("<", Column::lt),

    /**
     * The greater than or equal to operation.
     */
    GREATER_THAN_OR_EQUAL_TO(">=", Column::geq),

    /**
     * The greater than operation.
     */
    GREATER_THAN(">", Column::gt);

    @Nonnull
    private final String fhirPath;

    /**
     * A Spark function that can be used to execute this type of comparison for simple types.
     * Complex types such as Coding and Quantity will implement their own comparison functions.
     */
    @Nonnull
    private final BiFunction<Column, Column, Column> sparkFunction;

    ComparisonOperation(@Nonnull final String fhirPath,
        @Nonnull final BiFunction<Column, Column, Column> sparkFunction) {
      this.fhirPath = fhirPath;
      this.sparkFunction = sparkFunction;
    }

    @Nonnull
    public BiFunction<Column, Column, Column> getSparkFunction() {
      return sparkFunction;
    }

    @Override
    public String toString() {
      return fhirPath;
    }

  }
}

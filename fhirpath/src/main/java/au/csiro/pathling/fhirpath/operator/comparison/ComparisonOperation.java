package au.csiro.pathling.fhirpath.operator.comparison;

import jakarta.annotation.Nonnull;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;

/**
 * Represents a type of comparison operation.
 */
public enum ComparisonOperation {
  /**
   * The equals operation.
   */
  EQUALS("=", ColumnComparator::equalsTo),

  /**
   * The not equals operation.
   */
  NOT_EQUALS("!=", ColumnComparator::notEqual),

  /**
   * The less than or equal to operation.
   */
  LESS_THAN_OR_EQUAL_TO("<=", ColumnComparator::lessThanOrEqual),

  /**
   * The less than operation.
   */
  LESS_THAN("<", ColumnComparator::lessThan),

  /**
   * The greater than or equal to operation.
   */
  GREATER_THAN_OR_EQUAL_TO(">=", ColumnComparator::greaterThanOrEqual),

  /**
   * The greater than operation.
   */
  GREATER_THAN(">", ColumnComparator::greaterThan);

  @Nonnull
  private final String fhirPath;

  @Nonnull
  private final TriFunction<ColumnComparator, Column, Column, Column> operation;

  ComparisonOperation(@Nonnull final String fhirPath,
      @Nonnull final TriFunction<ColumnComparator, Column, Column, Column> operation) {
    this.fhirPath = fhirPath;
    this.operation = operation;
  }

  @Override
  @Nonnull
  public String toString() {
    return fhirPath;
  }

}

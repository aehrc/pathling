package au.csiro.pathling.sql.boundary;

/**
 * Common functionality for boundary functions for date and time.
 *
 * @author John Grimes
 */
public abstract class DateTimeBoundaryFunction {

  /**
   * Precision that includes all elements of a date time string.
   */
  protected static final int DATE_TIME_BOUNDARY_PRECISION = 17;

  /**
   * Precision that includes only the year, month and day of a date string.
   */
  protected static final int DATE_BOUNDARY_PRECISION = 8;

  /**
   * Precision that includes all components of a time string.
   */
  protected static final int TIME_BOUNDARY_PRECISION = 9;

}

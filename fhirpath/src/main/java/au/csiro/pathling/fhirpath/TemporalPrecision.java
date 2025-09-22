package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import lombok.Getter;

import java.time.temporal.ChronoUnit;

/**
 * Enumeration of supported temporal precision levels from year to millisecond.
 */
public enum TemporalPrecision {
  /**
   * Year precision (e.g., 2023)
   */
  YEAR(ChronoUnit.YEARS),

  /**
   * Month precision (e.g., 2023-06)
   */
  MONTH(ChronoUnit.MONTHS),

  /**
   * Day precision (e.g., 2023-06-15)
   */
  DAY(ChronoUnit.DAYS),

  /**
   * Hour precision (e.g., 2023-06-15T14)
   */
  HOUR(ChronoUnit.HOURS),

  /**
   * Minute precision (e.g., 2023-06-15T14:30)
   */
  MINUTE(ChronoUnit.MINUTES),

  /**
   * Second precision (e.g., 2023-06-15T14:30:45)
   */
  SECOND(ChronoUnit.SECONDS),

  /**
   * Up to nanoseconds precision (e.g., 2023-06-15T14:30:45.123456789)
   */
  FRACS(ChronoUnit.NANOS);

  @Getter
  @Nonnull
  private final ChronoUnit chronoUnit;

  TemporalPrecision(@Nonnull final ChronoUnit chronoUnit) {
    this.chronoUnit = chronoUnit;
  }

  /**
   * Returns true if the underlying ChronoUnit is time-based.
   *
   * @return true if the underlying ChronoUnit is time-based, false otherwise
   */
  public boolean isTimeBased() {
    return chronoUnit.isTimeBased();
  }
}

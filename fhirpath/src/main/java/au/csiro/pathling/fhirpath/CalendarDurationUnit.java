package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Enumeration of valid FHIRPath calendar duration units from year to millisecond.
 */
public enum CalendarDurationUnit {
  YEAR("year", false, "a"),
  MONTH("month", false, "mo"),
  WEEK("week", false, "wk"),
  DAY("day", false, "d"),
  HOUR("hour", false, "h"),
  MINUTE("minute", false, "min"),
  SECOND("second", true, "s"),
  MILLISECOND("millisecond", true, "ms");

  @Nonnull
  private final String unit;

  /**
   * Indicates whether the unit is a definite duration (i.e., has a fixed length in time). For
   * example seconds, and milliseconds are definite units, while years and
   * months are not because their lengths can vary (e.g., due to leap years or different month
   * lengths).
   */
  private final boolean definite;

  /**
   * The UCUM equivalent of the calendar duration unit.
   */
  @Nonnull
  private final String ucumEquivalent;

  private static final Map<String, CalendarDurationUnit> NAME_MAP = new HashMap<>();

  static {
    for (CalendarDurationUnit unit : values()) {
      NAME_MAP.put(unit.unit, unit);
      NAME_MAP.put(unit.unit + "s", unit); // plural
    }
  }

  CalendarDurationUnit(@Nonnull String name, boolean definite, @Nonnull String ucumEquivalent) {
    this.unit = name;
    this.definite = definite;
    this.ucumEquivalent = ucumEquivalent;
  }

  /**
   * Gets the canonical (that is singular) name of the calendar duration unit.
   *
   * @return the name of the unit
   */
  @Nonnull
  public String getUnit() {
    return unit;
  }

  /**
   * Indicates whether the unit is a definite duration (i.e., has a fixed length in time).
   * For example seconds, and milliseconds are definite units, while years and
   * months are not because their lengths can vary (e.g., due to leap years or different month
   * lengths).
   * @return true if the unit is definite, false otherwise
   */
  public boolean isDefinite() {
    return definite;
  }

  /**
   * Gets the UCUM equivalent of the calendar duration unit.
   *
   * @return the UCUM equivalent string
   */
  @Nonnull
  public String getUcumEquivalent() {
    return ucumEquivalent;
  }

  /**
   * Gets the CalendarDurationUnit from its string representation (case-insensitive, singular or
   * plural).
   *
   * @param name the name of the unit (e.g. "year", "years")
   * @return the corresponding CalendarDurationUnit
   * @throws IllegalArgumentException if the name is not valid
   */
  @Nonnull
  public static CalendarDurationUnit fromString(@Nonnull String name) {
    CalendarDurationUnit unit = NAME_MAP.get(name.toLowerCase(Locale.ROOT));
    if (unit == null) {
      throw new IllegalArgumentException("Unknown calendar duration unit: " + name);
    }
    return unit;
  }
}


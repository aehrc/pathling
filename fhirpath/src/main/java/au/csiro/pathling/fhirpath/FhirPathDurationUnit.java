package au.csiro.pathling.fhirpath;


import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

/**
 * Represents a unit of time duration in FHIRPath.
 */
@Getter
@AllArgsConstructor
public enum FhirPathDurationUnit {


  YEAR("year", Calendar.YEAR),
  MONTH("month", Calendar.MONTH),
  // Special case: week is not a standard Calendar unit so we use 7 days instead
  WEEK("week", Calendar.DATE, 7, 0),
  DAY("day", Calendar.DATE),
  HOUR("hour", Calendar.HOUR),
  MINUTE("minute", Calendar.MINUTE),
  // Special case: seconds can be added as decimal with 3 decimal places so we use milliseconds
  SECOND("second", Calendar.MILLISECOND, 1000, 3),
  MILLISECOND("millisecond", Calendar.MILLISECOND);
  // 

  FhirPathDurationUnit(@Nonnull final String unitName, final int calendarDuration) {
    this(unitName, calendarDuration, 1, 0);
  }

  /**
   * The name of the unit as it appears in FHIRPath in singular form.
   */
  @Nonnull
  private final String unitName;

  /**
   * The corresponding {@link Calendar} duration.
   */
  private final int calendarDuration;

  /**
   * The multiplier to convert values in the FHIRPath unit to the {@link Calendar} duration units .
   */
  @Getter(AccessLevel.PRIVATE)
  private final int multiplier;

  /**
   * The scale to use when converting values in the FHIRPath unit to the {@link Calendar} duration
   * units.
   */
  @Getter(AccessLevel.PRIVATE)
  private final int scale;

  /**
   * Converts a decimnal value in the FHIRPath unit to the corresponding {@link Calendar} duration
   * unit.
   *
   * @param value the value in the FHIRPath unit
   * @return the value in the {@link Calendar} duration unit
   */
  public int convertValueToUnit(@Nonnull final BigDecimal value) {
    return multiplier != 1
           ? value.setScale(scale, RoundingMode.FLOOR).multiply(BigDecimal.valueOf(multiplier))
               .intValue()
           : value.setScale(scale, RoundingMode.FLOOR).intValue();
  }

  private static final Map<String, FhirPathDurationUnit> UNIT_TO_DURATION_MAP = Arrays.stream(
          FhirPathDurationUnit.values())
      .flatMap(d -> Stream.of(Pair.of(d.getUnitName(), d), Pair.of(d.getUnitName() + "s", d)))
      .collect(Collectors.toUnmodifiableMap(Pair::getLeft, Pair::getRight));


  /**
   * Returns the {@link FhirPathDurationUnit} corresponding to the given unit name.
   *
   * @param unitName the unit name
   * @return the corresponding {@link FhirPathDurationUnit}
   */
  @Nonnull
  public static FhirPathDurationUnit fromUnitName(@Nonnull final String unitName) {
    final FhirPathDurationUnit duration = UNIT_TO_DURATION_MAP.get(unitName);
    checkUserInput(duration != null,
        "Unsupported calendar duration unit: " + unitName);
    return duration;
  }

}

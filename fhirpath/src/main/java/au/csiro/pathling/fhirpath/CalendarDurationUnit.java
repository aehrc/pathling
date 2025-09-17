package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Enumeration of valid FHIRPath calendar duration units from year to millisecond.
 */
public enum CalendarDurationUnit {
    YEAR("year"),
    MONTH("month"),
    DAY("day"),
    HOUR("hour"),
    MINUTE("minute"),
    SECOND("second"),
    MILLISECOND("millisecond");

    private final String name;

    private static final Map<String, CalendarDurationUnit> NAME_MAP = new HashMap<>();

    static {
        for (CalendarDurationUnit unit : values()) {
            NAME_MAP.put(unit.name, unit);
            NAME_MAP.put(unit.name + "s", unit); // plural
        }
    }

    CalendarDurationUnit(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Gets the CalendarDurationUnit from its string representation (case-insensitive, singular or plural).
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


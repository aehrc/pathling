package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.TemporalComparator;
import jakarta.annotation.Nonnull;

/**
 * An interface indicating that a class can be compared as a DateTime.
 * <p>
 * This includes both DateTime and Date, as per the FHIRPath specification. Also includes Instant,
 * which has to be specified at least up to seconds precision so should be comparable with DateTime
 * of the same precision.
 *
 * @author Piotr Szul
 */
public interface DateTimeComparable extends Comparable {

  ColumnComparator DATETIME_COMPARATOR = TemporalComparator.forDateTime();

  @Override
  @Nonnull
  default ColumnComparator getComparator() {
    return DATETIME_COMPARATOR;
  }

  @Override
  default boolean isComparableTo(@Nonnull final Comparable target) {
    return target instanceof DateTimeComparable;
  }

}

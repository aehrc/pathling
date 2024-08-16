package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.util.Optional;
import org.apache.spark.sql.Column;

public class QuantityCollection extends Collection {

  public QuantityCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  public static Collection fromUcumLiteral(final String value, final String unit) {
    return null;
  }

  public static Collection fromCalendarDurationLiteral(final String value, final String duration) {
    return null;
  }

}

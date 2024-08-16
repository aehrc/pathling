package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.text.ParseException;
import java.util.Optional;
import org.apache.spark.sql.Column;

public class DateCollection extends Collection {

  public DateCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  public static Collection fromLiteral(final String fhirPath) throws ParseException {
    return null;
  }

}

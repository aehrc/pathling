package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.util.Optional;
import org.apache.spark.sql.Column;

public class DecimalCollection extends Collection {

  public DecimalCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  public static Collection fromLiteral(final String fhirPath) throws NumberFormatException {
    return null;
  }

}

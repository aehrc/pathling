package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.util.Optional;
import org.apache.spark.sql.Column;

public class BooleanCollection extends Collection {

  public BooleanCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  public static Collection fromLiteral(final String fhirPath) {
    return null;
  }

}

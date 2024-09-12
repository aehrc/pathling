package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

public class StringCollection extends Collection {

  private StringCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  @NotNull
  public static Collection fromLiteral(@NotNull final String fhirPath) {
    return new StringCollection(functions.lit(fhirPath), Optional.of(FhirPathType.STRING));
  }
  
}

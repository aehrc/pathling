package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

public class ResourceCollection extends Collection {

  public ResourceCollection(final Column column, final Optional<FhirPathType> type) {
    super(column, type);
  }

  @Override
  public @NotNull Collection traverse(final @NotNull String elementName) {
    return super.traverse(elementName);
  }

}

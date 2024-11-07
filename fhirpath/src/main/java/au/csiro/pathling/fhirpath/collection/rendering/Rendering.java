package au.csiro.pathling.fhirpath.collection.rendering;

import java.util.Optional;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

public interface Rendering {

  @NotNull
  Optional<Column> getColumn();

  @NotNull
  Optional<Column> getField(@NotNull String name);

}

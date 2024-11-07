package au.csiro.pathling.fhirpath.collection.rendering;

import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * This is used for rendering a collection that can be represented by a single column. The column
 * can be of any type. If it is a struct, you can use getField on it to access specific fields
 * within the struct.
 *
 * @param column the column representing the collection
 */
public record SingleColumnRendering(@NotNull Column column) implements Rendering {

  @Override
  public @NotNull Optional<Column> getColumn() {
    return Optional.of(column);
  }

  @Override
  public @NotNull Optional<Column> getField(@NotNull final String name) {
    // If the column is not resolved or is not a struct, we can't access fields within it.
    if (!column.expr().resolved() || !(column.expr()
        .dataType() instanceof final StructType structType)) {
      return Optional.empty();
    }
    // If the struct contains the field, return it. Otherwise, return empty.
    return structType.contains(name)
           ? Optional.of(column.getField(name))
           : Optional.empty();
  }

}

package au.csiro.pathling.fhirpath.collection.rendering;

import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * This is used for rendering a collection that can be represented by a single column. The column
 * can be of any type. If it is a struct, you can use traverse on it to access specific fields
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
  public @NotNull Optional<Rendering> traverse(@NotNull final String name) {
    // If the column is not resolved or is not a struct, we can't access fields within it.
    if (!column.expr().resolved() || !(column.expr()
        .dataType() instanceof final StructType structType)) {
      return Optional.empty();
    }
    // If the struct contains the field, return it. Otherwise, return empty.
    return structType.contains(name)
           ? Optional.of(column.getField(name)).map(SingleColumnRendering::new)
           : Optional.empty();
  }

  @Override
  public @NotNull Rendering singleton() {
    return this.map(c -> {
      final Expression columnExpression = c.expr();
      if (columnExpression.resolved()
          && columnExpression.dataType() instanceof org.apache.spark.sql.types.ArrayType) {
        // If the column is an array, we can check its size and enforce the rules here:
        // https://hl7.org/fhirpath/N1/#singleton-evaluation-of-collections
        return functions.when(functions.size(c).leq(1), functions.get(c, functions.lit(0)))
            .otherwise(functions.raise_error(functions.lit("Expected a singular input")));
      }
      // If the column is not resolved or not an array, assume that it is already a singular value.
      return c;
    });
  }

  @Override
  public @NotNull Rendering collection() {
    return this.map(c -> {
      final Expression columnExpression = c.expr();
      if (columnExpression.resolved()) {
        if (columnExpression.dataType() instanceof org.apache.spark.sql.types.ArrayType) {
          // If the column is resolved and it is an array, return it as-is.
          return c;
        } else {
          // If the column is resolved and not an array, turn it into one.
          return functions.array(c);
        }
      }
      // If the column is not resolved, assume it is singular and turn it into an array.
      return functions.array(c);
    });
  }

  @Override
  public @NotNull Rendering map(@NotNull final UnaryOperator<Column> mapper) {
    return getColumn()
        .map(mapper)
        .map(SingleColumnRendering::new)
        .orElse(this);
  }

}

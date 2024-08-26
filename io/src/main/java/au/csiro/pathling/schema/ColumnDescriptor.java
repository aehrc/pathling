package au.csiro.pathling.schema;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

/**
 * Information about a column within a {@link org.apache.spark.sql.Dataset}.
 *
 * @param column the column
 * @param name the name of the column
 * @param type the data type of the column
 */
public record ColumnDescriptor(@NotNull Column column, @NotNull String name,
                               @NotNull DataType type) {

}

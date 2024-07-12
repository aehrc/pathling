package au.csiro.pathling.schema;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

public record ColumnDescriptor(@Nonnull Column column, @Nonnull String name,
                               @Nonnull DataType type) {

}

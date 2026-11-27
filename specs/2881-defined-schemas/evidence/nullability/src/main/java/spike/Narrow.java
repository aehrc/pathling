package spike;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

/** Narrowing: conform to a schema with NOT NULL fields. */
public final class Narrow {
  private Narrow() {}

  public static Dataset<Row> apply(Dataset<Row> df, StructType t) {
    return Conform.apply(df, t);
  }

  public static Column top(String n, org.apache.spark.sql.types.DataType t) {
    return Conform.top(n, t);
  }
}

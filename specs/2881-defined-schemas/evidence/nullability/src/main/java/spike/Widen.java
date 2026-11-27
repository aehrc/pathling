package spike;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

/** Widening: conform to a schema with nullable fields. */
public final class Widen {
  private Widen() {}

  public static Dataset<Row> apply(Dataset<Row> df, StructType t) {
    return Conform.apply(df, t);
  }
}

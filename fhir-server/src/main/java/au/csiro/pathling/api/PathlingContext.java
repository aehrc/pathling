package au.csiro.pathling.api;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;




public class PathlingContext {

  private final String serverUrl;

  private PathlingContext(String serverUrl) {
    this.serverUrl = serverUrl;
  }

  @Nonnull
  public Dataset<Row> memberOf(@Nonnull final Dataset<Row> codingDataframe,
      @Nonnull final Column codingColumn, @Nonnull final String valueSetUrl, @Nonnull final String outputColumnName) {
    return codingDataframe.withColumn(outputColumnName, functions.lit(true));
  }

  public static PathlingContext create(@Nonnull final String serverUrl) {
    return new PathlingContext(serverUrl);
  }
}

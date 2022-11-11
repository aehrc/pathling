package au.csiro.pathling.spark;

import org.apache.spark.sql.SparkSession;
import javax.annotation.Nonnull;

public interface SparkConfigurer {

  void configure(@Nonnull final SparkSession spark);
}

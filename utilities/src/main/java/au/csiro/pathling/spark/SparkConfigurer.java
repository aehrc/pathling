package au.csiro.pathling.spark;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;

/**
 * Common interface for functions/classes that can configure {@link SparkSession}.
 */
@FunctionalInterface
public interface SparkConfigurer {

  /**
   * Configure some aspect of {@link SparkSession}
   *
   * @param spark the {@link SparkSession} to configure.
   */
  void configure(@Nonnull final SparkSession spark);
}

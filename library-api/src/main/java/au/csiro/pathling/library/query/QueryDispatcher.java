package au.csiro.pathling.library.query;

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A a single point for running queries. This exists to decouple the execution of queries from the
 * dispatch, allowing for easier testing and potentially extending the number of types of queries
 * that can be run in the future.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public interface QueryDispatcher {

  /**
   * Dispatches the given view request to be executed and returns the result.
   *
   * @param view the request to execute
   * @return the result of the execution
   */
  @Nonnull
  Dataset<Row> dispatch(@Nonnull FhirView view);
 
}

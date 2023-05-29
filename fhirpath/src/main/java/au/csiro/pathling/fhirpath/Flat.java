package au.csiro.pathling.fhirpath;

import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Designates an expression that is capable of being rendered into a flat, unstructured
 * representation such as CSV.
 *
 * @author John Grimes
 */
public interface Flat {

  /**
   * @return a {@link Column} within the dataset containing the values of the nodes
   */
  @Nonnull
  Column getValueColumn();

}
